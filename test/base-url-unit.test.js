import test from "node:test";
import assert from "node:assert/strict";
import { cleanHostHeader, parseAllowedHosts, isAllowedHost, resolveExternalBaseUrl } from "../src/base-url.js";

const DEFAULT = "http://localhost:8787";
const allowed = parseAllowedHosts("abm.example.com, Other.Example.com:8443, [::1], bad host, evil.example/path");

function resolve(headers, extra = {}) {
  return resolveExternalBaseUrl({ appBaseUrl: "", allowedHosts: allowed, defaultBaseUrl: DEFAULT, headers, ...extra });
}

test("cleanHostHeader accepts clean hosts and rejects everything else", () => {
  assert.equal(cleanHostHeader("Abm.Example.com"), "abm.example.com");
  assert.equal(cleanHostHeader("abm.example.com:8443, second.example"), "abm.example.com:8443");
  assert.equal(cleanHostHeader("[::1]:8787"), "[::1]:8787");
  // Whitespace at the two ends is trimmed; what is left is still a clean host.
  assert.equal(cleanHostHeader("  abm.example.com\r\n"), "abm.example.com");
  for (const bad of [
    "", undefined, null, 5, "evil.example/path", "evil.example?x=1", "a@evil.example", "evil example",
    "evil.example\r\nSet-Cookie: x=1", "evil.example\rx", "evil.example\nx", "evil.example\tx", "-evil", "evil.example:99999999",
    "<b>x</b>", "javascript:alert(1)", '"><i x=1>', "<svg onload=alert(1)>", "evil.example#frag"
  ]) {
    assert.equal(cleanHostHeader(bad), null, JSON.stringify(bad));
  }
});

test("allowlist entries are cleaned; bad entries are dropped", () => {
  assert.deepEqual([...allowed].sort(), ["[::1]", "abm.example.com", "other.example.com:8443"]);
  assert.equal(parseAllowedHosts("").size, 0);
  assert.equal(parseAllowedHosts(undefined).size, 0);
});

test("a host without a port matches any port; a host with a port matches only that port", () => {
  assert.equal(isAllowedHost("abm.example.com:9999", allowed), true);
  assert.equal(isAllowedHost("other.example.com:8443", allowed), true);
  assert.equal(isAllowedHost("other.example.com:1", allowed), false);
  assert.equal(isAllowedHost("other.example.com", allowed), false);
  assert.equal(isAllowedHost("abm.example.com", new Set()), false);
});

test("resolveExternalBaseUrl: APP_BASE_URL wins and ignores every header", () => {
  const url = resolveExternalBaseUrl({ appBaseUrl: "https://app.example", allowedHosts: allowed, defaultBaseUrl: DEFAULT, headers: { host: "evil.example", "x-forwarded-host": "abm.example.com" } });
  assert.equal(url, "https://app.example");
});

test("resolveExternalBaseUrl: hostile values, including CR LF, give the default", () => {
  for (const h of [
    "evil.example", "evil.example/steal", "abm.example.com/x", "abm.example.com\r\nX: y", "abm.example.com\nSet-Cookie: a=b",
    "good@abm.example.com", "evil.example, abm.example.com", "<b>x</b>"
  ]) {
    assert.equal(resolve({ host: h }), DEFAULT, JSON.stringify(h));
    assert.equal(resolve({ "x-forwarded-host": h }), DEFAULT, JSON.stringify(h));
  }
  assert.equal(resolve({}), DEFAULT);
});

test("resolveExternalBaseUrl: listed hosts are used; the scheme is only http or https", () => {
  assert.equal(resolve({ "x-forwarded-host": "abm.example.com", "x-forwarded-proto": "https" }), "https://abm.example.com");
  assert.equal(resolve({ host: "ABM.example.com:3000" }), "http://abm.example.com:3000");
  assert.equal(resolve({ "x-forwarded-host": "abm.example.com", "x-forwarded-proto": "javascript" }), "http://abm.example.com");
  assert.equal(resolve({ "x-forwarded-host": "abm.example.com", "x-forwarded-proto": "https, http" }), "https://abm.example.com");
  // X-Forwarded-Host wins over Host, as before; an unlisted one is not rescued by a listed Host.
  assert.equal(resolve({ host: "abm.example.com", "x-forwarded-host": "evil.example" }), DEFAULT);
});
