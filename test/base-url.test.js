import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import http from "node:http";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

/*
  C-ABM-02: when APP_BASE_URL is not set, the server must never build its external
  address from X-Forwarded-Host or Host unless the value is on the ALLOWED_HOSTS list.
  These tests start the real server.js as a child process (no database, auth enabled with
  a dummy verification setting, no .env file in the repo) and read two answers that carry
  the external address: GET /.well-known/oauth-protected-resource and the WWW-Authenticate
  header of an unauthenticated POST /mcp.
*/

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
    s.on("error", reject);
  });
}

async function startServer(extraEnv = {}) {
  const port = await freePort();
  const env = {
    PATH: process.env.PATH,
    SystemRoot: process.env.SystemRoot,
    PORT: String(port),
    // A dummy value only so the server does not answer 500 "AuthMisconfigured"; no token is ever valid.
    CLERK_JWT_KEY: "dummy-not-a-real-verification-setting",
    ...extraEnv
  };
  const child = spawn(process.execPath, ["server.js"], { cwd: root, env });
  let output = "";
  child.stdout.on("data", (d) => (output += d));
  child.stderr.on("data", (d) => (output += d));
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("server did not start: " + output)), 15000);
    const check = () => {
      if (output.includes("ABM app running")) {
        clearTimeout(timer);
        resolve();
      } else setTimeout(check, 50);
    };
    child.on("exit", () => reject(new Error("server exited early: " + output)));
    check();
  });
  return {
    port,
    output: () => output,
    stop: () => new Promise((resolve) => {
      child.once("exit", resolve);
      child.kill();
    })
  };
}

function request(port, path, { method = "GET", headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: "127.0.0.1", port, path, method, headers }, (res) => {
      let body = "";
      res.on("data", (d) => (body += d));
      res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body }));
    });
    req.on("error", reject);
    if (method === "POST") req.write("{}");
    req.end();
  });
}

// Raw socket, for header bytes the http client refuses to send (CR, LF, tabs).
function rawRequest(port, text) {
  return new Promise((resolve) => {
    const sock = net.connect(port, "127.0.0.1", () => sock.write(text));
    let data = "";
    sock.on("data", (d) => (data += d));
    sock.on("close", () => resolve(data));
    sock.on("error", () => resolve(data));
    setTimeout(() => sock.destroy(), 1500);
  });
}

async function probe(port, headers) {
  const meta = await request(port, "/.well-known/oauth-protected-resource", { headers });
  const challenge = await request(port, "/mcp", { method: "POST", headers: { "content-type": "application/json", ...headers } });
  return {
    metaBody: meta.body,
    metaStatus: meta.status,
    challengeStatus: challenge.status,
    wwwAuthenticate: String(challenge.headers["www-authenticate"] ?? "")
  };
}

const HOSTILE = [
  { name: "evil host in Host", headers: { Host: "evil.example" }, bad: "evil.example" },
  { name: "evil host in X-Forwarded-Host", headers: { "X-Forwarded-Host": "evil.example" }, bad: "evil.example" },
  { name: "evil host with a path", headers: { "X-Forwarded-Host": "evil.example/steal?x=1" }, bad: "evil.example" },
  { name: "evil host with userinfo", headers: { "X-Forwarded-Host": "good.example@evil.example" }, bad: "evil.example" },
  { name: "evil host first in a list", headers: { "X-Forwarded-Host": "evil.example, other.example" }, bad: "evil.example" },
  { name: "evil scheme in X-Forwarded-Proto", headers: { "X-Forwarded-Host": "evil.example", "X-Forwarded-Proto": "javascript" }, bad: "javascript" },
  { name: "evil Host plus evil X-Forwarded-Host", headers: { Host: "evil.example", "X-Forwarded-Host": "evil2.example" }, bad: "evil" }
];

test("with neither APP_BASE_URL nor ALLOWED_HOSTS: hostile headers never reach the metadata or the challenge", async () => {
  const s = await startServer();
  try {
    for (const h of HOSTILE) {
      const r = await probe(s.port, h.headers);
      assert.equal(r.metaStatus, 200, h.name);
      assert.equal(r.challengeStatus, 401, h.name);
      assert.ok(!r.metaBody.includes(h.bad), `${h.name}: metadata contains ${h.bad}: ${r.metaBody}`);
      assert.ok(!r.wwwAuthenticate.includes(h.bad), `${h.name}: challenge contains ${h.bad}: ${r.wwwAuthenticate}`);
    }
    // The fixed safe default is used.
    const r = await probe(s.port, { "X-Forwarded-Host": "evil.example" });
    const meta = JSON.parse(r.metaBody);
    assert.equal(meta.resource, `http://localhost:${s.port}/mcp`);
    assert.equal(meta.resource_documentation, `http://localhost:${s.port}/`);
    assert.ok(r.wwwAuthenticate.includes(`resource_metadata="http://localhost:${s.port}/.well-known/oauth-protected-resource"`));
  } finally {
    await s.stop();
  }
});

test("with neither APP_BASE_URL nor ALLOWED_HOSTS: a CR LF value in a header never reaches the answers", async () => {
  const s = await startServer();
  try {
    const attempts = [
      "GET /.well-known/oauth-protected-resource HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Forwarded-Host: evil.example\r\nX-Injected: 1\r\nConnection: close\r\n\r\n",
      "GET /.well-known/oauth-protected-resource HTTP/1.1\r\nHost: evil.example\rSet-Cookie: pwn=1\r\nConnection: close\r\n\r\n",
      "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Forwarded-Host: evil.example\r\n\tmore.example\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
    ];
    for (const a of attempts) {
      const reply = await rawRequest(s.port, a);
      assert.ok(!reply.includes("evil.example"), "reply contains evil.example: " + reply);
      assert.ok(!/set-cookie:\s*pwn/i.test(reply), "reply carries an injected header: " + reply);
    }
    // The server is still up after the hostile requests.
    const ok = await request(s.port, "/health");
    assert.equal(ok.status, 200);
  } finally {
    await s.stop();
  }
});

test("with neither setting: logs one warning saying the default is used", async () => {
  const s = await startServer();
  try {
    await probe(s.port, { "X-Forwarded-Host": "evil.example" });
    await probe(s.port, { Host: "evil.example" });
    const warnings = s.output().split("\n").filter((l) => /WARNING/.test(l) && /APP_BASE_URL/.test(l));
    assert.equal(warnings.length, 1, s.output());
    assert.ok(/ALLOWED_HOSTS/.test(warnings[0]));
    assert.ok(!s.output().includes("evil.example"), "hostile value must not be logged");
  } finally {
    await s.stop();
  }
});

test("with ALLOWED_HOSTS and no APP_BASE_URL: a listed host is used, an unlisted one falls back to the default", async () => {
  const s = await startServer({ ALLOWED_HOSTS: "abm.example.com, other.example.com:8443" });
  try {
    const okXfh = await probe(s.port, { "X-Forwarded-Host": "abm.example.com", "X-Forwarded-Proto": "https" });
    assert.equal(JSON.parse(okXfh.metaBody).resource, "https://abm.example.com/mcp");
    assert.ok(okXfh.wwwAuthenticate.includes('resource_metadata="https://abm.example.com/.well-known/oauth-protected-resource"'));

    const okHost = await probe(s.port, { Host: "OTHER.example.com:8443" });
    assert.equal(JSON.parse(okHost.metaBody).resource, "http://other.example.com:8443/mcp");

    for (const h of HOSTILE) {
      const r = await probe(s.port, h.headers);
      assert.ok(!r.metaBody.includes(h.bad), `${h.name}: metadata contains ${h.bad}`);
      assert.ok(!r.wwwAuthenticate.includes(h.bad), `${h.name}: challenge contains ${h.bad}`);
    }
    // A listed host with a hostile scheme: the scheme is not taken from the header.
    const badProto = await probe(s.port, { "X-Forwarded-Host": "abm.example.com", "X-Forwarded-Proto": "javascript" });
    assert.ok(JSON.parse(badProto.metaBody).resource.startsWith("http://abm.example.com/"));
    // No "neither set" warning when an allowlist is set.
    assert.ok(!/WARNING.*APP_BASE_URL/.test(s.output()));
  } finally {
    await s.stop();
  }
});

test("with APP_BASE_URL set: behaviour is exactly as before (header values never matter, trailing slashes trimmed)", async () => {
  const s = await startServer({ APP_BASE_URL: "https://abm.gtmhelix.example///", ALLOWED_HOSTS: "abm.example.com" });
  try {
    for (const headers of [{}, { "X-Forwarded-Host": "evil.example" }, { "X-Forwarded-Host": "abm.example.com" }, { Host: "evil.example" }]) {
      const r = await probe(s.port, headers);
      const meta = JSON.parse(r.metaBody);
      assert.equal(meta.resource, "https://abm.gtmhelix.example/mcp");
      assert.equal(meta.resource_documentation, "https://abm.gtmhelix.example/");
      assert.deepEqual(meta.bearer_methods_supported, ["header"]);
      assert.ok(r.wwwAuthenticate.startsWith('Bearer realm="GTMHelix ABM", resource_metadata="https://abm.gtmhelix.example/.well-known/oauth-protected-resource"'));
      assert.ok(r.wwwAuthenticate.endsWith(', error="invalid_token"'));
    }
    assert.ok(!/WARNING.*APP_BASE_URL/.test(s.output()));
  } finally {
    await s.stop();
  }
});

test("a Host value that is not a valid URL host does not crash the server", async () => {
  const s = await startServer();
  try {
    for (const hostValue of ["a b", "evil.example:notaport", "[", "a<b>c", "x y z"]) {
      const reply = await rawRequest(s.port, `GET /health HTTP/1.1\r\nHost: ${hostValue}\r\nConnection: close\r\n\r\n`);
      assert.ok(!reply.includes("evil.example") || !reply.includes("resource"), reply);
      // The process must still answer afterwards.
      const ok = await request(s.port, "/health");
      assert.equal(ok.status, 200, `server stopped answering after Host: ${hostValue}
${s.output()}`);
    }
  } finally {
    await s.stop();
  }
});
