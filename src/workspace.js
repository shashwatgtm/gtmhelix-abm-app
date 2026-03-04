import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";

const DATA_DIR = "data";
const WORKSPACE_DB = join(DATA_DIR, "workspaces.json");

function ensureWorkspaceFileDb() {
  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
  if (!existsSync(WORKSPACE_DB)) writeFileSync(WORKSPACE_DB, JSON.stringify({ workspaces: {} }, null, 2), "utf8");
}

export async function initWorkspaceStore({ pool }) {
  if (!pool) return;
  // Table creation is handled in server initPg; keep this as a no-op guard for future refactors.
}

export async function resolveWorkspaceId({ tenantId, pool }) {
  if (!tenantId) throw new Error("Missing tenantId");

  // Postgres-backed mapping
  if (pool) {
    const found = await pool.query(`SELECT workspace_id FROM abm_workspaces WHERE tenant_id = $1`, [tenantId]);
    if (found.rows?.[0]?.workspace_id) return found.rows[0].workspace_id;

    const ws = randomUUID();
    await pool.query(`INSERT INTO abm_workspaces(tenant_id, workspace_id) VALUES($1,$2)`, [tenantId, ws]);
    return ws;
  }

  // File-backed mapping
  ensureWorkspaceFileDb();
  const db = JSON.parse(readFileSync(WORKSPACE_DB, "utf8"));
  db.workspaces = db.workspaces || {};
  if (db.workspaces[tenantId]) return db.workspaces[tenantId];

  const ws = randomUUID();
  db.workspaces[tenantId] = ws;
  writeFileSync(WORKSPACE_DB, JSON.stringify(db, null, 2), "utf8");
  return ws;
}
