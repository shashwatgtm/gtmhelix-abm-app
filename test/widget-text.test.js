import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

/*
  These tests load the real script from public/abm-widget.html into node:vm with a
  small document stub (no new package). The stub records every innerHTML write, every
  createElement tag and every attribute, so the tests can show that user-controlled
  values (client name, project id, field-error keys and messages) only ever reach the
  page as text nodes, never as HTML.
*/

const here = dirname(fileURLToPath(import.meta.url));
const html = readFileSync(join(here, "..", "public", "abm-widget.html"), "utf8");
const scriptMatch = html.match(/<script type="module">([\s\S]*?)<\/script>/);
assert.ok(scriptMatch, "widget script block not found");
const widgetScript = scriptMatch[1];

// Inert strings. They are never executed; the tests only check that they stay text.
const PAYLOADS = [
  "<b>x</b>",
  '"><i x=1>',
  "<img src=x onerror=alert(1)>",
  "javascript:alert(1)",
  "<svg onload=alert(1)>"
];

class TextNode {
  constructor(text) {
    this.nodeType = 3;
    this.data = String(text);
  }
  get textContent() {
    return this.data;
  }
}

class El {
  constructor(tag, log) {
    this.nodeType = 1;
    this.tagName = tag.toLowerCase();
    this.log = log;
    this.children = [];
    this.attributes = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.id = "";
    log.tags.push(this.tagName);
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  setAttribute(name, value) {
    this.attributes[name] = String(value);
    this.log.attrs.push({ tag: this.tagName, name, value: String(value) });
  }
  getAttribute(name) {
    return name in this.attributes ? this.attributes[name] : null;
  }
  set innerHTML(value) {
    this.log.innerHTML.push(String(value));
    this.children = [];
  }
  get innerHTML() {
    return "";
  }
  set textContent(value) {
    this.children = [];
    const s = String(value ?? "");
    if (s !== "") this.children.push(new TextNode(s));
  }
  get textContent() {
    return this.children.map((c) => c.textContent).join("");
  }
  descendants() {
    const out = [];
    for (const c of this.children) {
      if (c.nodeType === 1) {
        out.push(c, ...c.descendants());
      }
    }
    return out;
  }
  matches(selector) {
    let m = selector.match(/^#([\w-]+)$/);
    if (m) return this.id === m[1];
    m = selector.match(/^([a-z0-9]+)(?:\[([\w-]+)\])?$/i);
    if (!m) return false;
    if (this.tagName !== m[1].toLowerCase()) return false;
    return m[2] ? m[2] in this.attributes : true;
  }
  querySelectorAll(selector) {
    return this.descendants().filter((d) => d.matches(selector));
  }
  querySelector(selector) {
    return this.querySelectorAll(selector)[0] ?? null;
  }
}

function makeWidget(callToolImpl) {
  const log = { tags: [], attrs: [], innerHTML: [], alerts: [], calls: [] };
  const byId = new Map();
  const document = {
    createElement: (tag) => new El(tag, log),
    createTextNode: (text) => new TextNode(text),
    querySelector(selector) {
      const id = selector.startsWith("#") ? selector.slice(1) : selector;
      if (!byId.has(id)) {
        const el = new El("div", log);
        el.id = id;
        byId.set(id, el);
      }
      return byId.get(id);
    }
  };
  const window = {
    openai: {
      widgetState: { workspaceId: "ws_test" },
      setWidgetState() {},
      async callTool(name, payload) {
        log.calls.push(name);
        return callToolImpl(name, payload);
      }
    }
  };
  const context = vm.createContext({
    window,
    document,
    navigator: { clipboard: { writeText: async () => {} } },
    alert: (m) => log.alerts.push(String(m)),
    crypto: { randomUUID: () => "uuid-test" },
    console
  });
  // The script is a module with top-level await and no imports; run it inside an async function.
  const done = vm.runInContext(`(async () => {\n${widgetScript}\n})()`, context);
  const el = (id) => document.querySelector("#" + id);
  // Reset the tag log after the initial render so each test measures only its own action.
  return { log, el, done };
}

const ALLOWED_TAGS = new Set(["div", "span", "b", "br", "table", "thead", "tbody", "tr", "th", "td", "button", "input", "label", "textarea", "select", "option"]);

test("field-error list: keys and messages stay text, never HTML", async () => {
  const fieldErrors = {};
  PAYLOADS.forEach((p, i) => {
    fieldErrors[p] = [p, "plain message " + i, p + p];
  });
  const w = makeWidget(async (name) => {
    if (name === "validate_abm_intake") return { structuredContent: { fieldErrors } };
    return { structuredContent: {} };
  });
  await w.done;
  const errorsEl = w.el("errors");
  const beforeInner = w.log.innerHTML.length;
  await w.el("validateBtn").onclick();

  // No user value ever reaches innerHTML.
  const writes = w.log.innerHTML.slice(beforeInner);
  for (const payload of PAYLOADS) {
    for (const v of writes) {
      assert.ok(!v.includes(payload), `payload reached innerHTML: ${JSON.stringify(v)}`);
    }
  }

  // The list shows the exact text the server sent.
  const expected =
    "Fix these:" +
    Object.keys(fieldErrors)
      .map((k) => `${k}: ${fieldErrors[k].join(" | ")}`)
      .join("");
  assert.equal(errorsEl.textContent, expected);
  assert.equal(errorsEl.style.display, "block");

  // Same look: a bold heading, a line break, then one div per key holding a span.mono.
  const kids = errorsEl.children;
  assert.equal(kids[0].tagName, "b");
  assert.equal(kids[0].textContent, "Fix these:");
  assert.equal(kids[1].tagName, "br");
  const lines = kids.slice(2);
  assert.equal(lines.length, PAYLOADS.length);
  lines.forEach((line, i) => {
    assert.equal(line.tagName, "div");
    const span = line.children[0];
    assert.equal(span.tagName, "span");
    assert.equal(span.className, "mono");
    assert.equal(span.textContent, PAYLOADS[i]);
  });

  // Payload strings created no elements of their own: every descendant is one the widget makes itself.
  for (const d of errorsEl.descendants()) {
    assert.ok(["b", "br", "div", "span"].includes(d.tagName), `unexpected element ${d.tagName}`);
    assert.deepEqual(Object.keys(d.attributes), []);
  }
});

test("field-error list: clearing the errors leaves it empty and hidden", async () => {
  const w = makeWidget(async () => ({ structuredContent: { fieldErrors: {} } }));
  await w.done;
  await w.el("validateBtn").onclick();
  assert.equal(w.el("errors").style.display, "none");
  assert.equal(w.el("errors").textContent, "");
});

test("projects table: client name, project id and dates stay text", async () => {
  const rows = PAYLOADS.map((p, i) => ({
    clientName: p,
    projectId: p + "_id" + i,
    createdAt: "2026-01-0" + (i + 1) + "T10:00:00.000Z"
  }));
  // One row whose date is itself a payload (becomes the text "Invalid Date").
  rows.push({ clientName: "last", projectId: "<b>pid</b>", createdAt: "<b>x</b>" });

  const w = makeWidget(async (name) => {
    if (name === "list_abm_projects") return { structuredContent: { projects: rows } };
    return { structuredContent: {} };
  });
  await w.done;
  const table = w.el("projectsTable");
  const beforeInner = w.log.innerHTML.length;
  const tagsBefore = w.log.tags.length;
  await w.el("tabProjects").onclick();

  // No user value ever reaches innerHTML.
  const writes = w.log.innerHTML.slice(beforeInner);
  for (const r of rows) {
    for (const v of writes) {
      assert.ok(!v.includes(r.clientName) && !v.includes(r.projectId), `row value reached innerHTML: ${JSON.stringify(v)}`);
    }
  }

  // Same structure as before: thead with 4 headings, tbody with one tr per row.
  const thead = table.children.find((c) => c.tagName === "thead");
  const tbody = table.children.find((c) => c.tagName === "tbody");
  assert.ok(thead && tbody, "table needs thead and tbody");
  assert.deepEqual(
    thead.querySelectorAll("th").map((t) => t.textContent),
    ["Client", "Created", "Project ID", "Actions"]
  );
  const trs = tbody.children.filter((c) => c.tagName === "tr");
  assert.equal(trs.length, rows.length);

  trs.forEach((tr, i) => {
    const tds = tr.children.filter((c) => c.tagName === "td");
    assert.equal(tds.length, 4);
    assert.equal(tds[0].textContent, rows[i].clientName);
    assert.equal(tds[1].textContent, new Date(rows[i].createdAt).toLocaleString());
    assert.equal(tds[2].className, "mono");
    assert.equal(tds[2].textContent, rows[i].projectId);
    const buttons = tds[3].children.filter((c) => c.tagName === "button");
    assert.deepEqual(buttons.map((b) => b.className), ["secondary", "danger"]);
    assert.deepEqual(buttons.map((b) => b.textContent), ["Load", "Delete"]);
    assert.equal(buttons[0].getAttribute("data-load"), rows[i].projectId);
    assert.equal(buttons[1].getAttribute("data-del"), rows[i].projectId);
  });

  // Payload strings created no elements or attributes of their own.
  for (const tag of w.log.tags.slice(tagsBefore)) {
    assert.ok(ALLOWED_TAGS.has(tag), `unexpected element ${tag}`);
  }
  for (const a of table.descendants().flatMap((d) => Object.keys(d.attributes))) {
    assert.ok(["data-load", "data-del"].includes(a), `unexpected attribute ${a}`);
  }
});

test("projects table: Load and Delete buttons still send the exact project id", async () => {
  const id = '"><i x=1>_proj';
  const sent = [];
  const w = makeWidget(async (name, payload) => {
    sent.push([name, payload.projectId]);
    if (name === "list_abm_projects") {
      return { structuredContent: { projects: [{ clientName: "<b>c</b>", projectId: id, createdAt: "2026-01-01T00:00:00Z" }] } };
    }
    return { structuredContent: {} };
  });
  await w.done;
  await w.el("tabProjects").onclick();
  const table = w.el("projectsTable");
  await table.querySelector("button[data-load]").onclick();
  await table.querySelector("button[data-del]").onclick();
  assert.deepEqual(
    sent.filter(([n]) => n === "get_abm_project" || n === "delete_abm_project"),
    [["get_abm_project", id], ["delete_abm_project", id]]
  );
});
