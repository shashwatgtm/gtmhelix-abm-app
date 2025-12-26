import { createServer } from "node:http";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import pg from "pg";

const widgetHtml = readFileSync("public/abm-widget.html", "utf8");

const SEGMENTS = [
  "Midmarket ITeS/Telecom",
  "B2B SaaS Startup (Series A/B+)",
  "GenAI Startup",
  "Vertical SaaS"
];

const VERTICALS = [
  "Logistics",
  "Cybersecurity",
  "Fintech",
  "HRtech",
  "TravelTech",
  "CPG",
  "Pharma",
  "Healthcare",
  "Other"
];

const GEOS = [
  "US", "UK", "India", "Singapore", "Indonesia",
  "DACH", "Nordics", "UAE", "APAC", "MENA"
];

const intakeSchema = z.object({
  clientName: z.string().min(2),
  website: z.string().url().optional(),
  segment: z.enum(SEGMENTS),
  verticals: z.array(z.enum(VERTICALS)).min(1),
  otherVertical: z.string().optional(),
  geos: z.array(z.enum(GEOS)).min(1),
  productCategory: z.string().min(3),
  oneLiner: z.string().min(40), // anti-vague gate
  primaryBuyerRoles: z.array(z.string().min(3)).min(1),
  pains: z.array(z.string().min(20)).min(2),
  differentiation: z.array(z.string().min(20)).min(2),
  competitors: z.array(z.string().min(2)).min(1),
  avgContractValueUsd: z.number().int().min(1000).optional(),
  targetHeadcountBand: z.enum(["1-50", "51-200", "201-1000", "1001-5000", "5000+"]),
  salesMotion: z.enum(["Sales-led", "PLG", "Hybrid", "Partner-led"]),
  salesCycleDays: z.number().int().min(7).max(540),
  dataSignalsAvailable: z.array(z.enum(["Intent", "Technographics", "Hiring", "Funding", "Website activity", "None"])).min(1)
}).superRefine((val, ctx) => {
  if (val.verticals.includes("Other") && (!val.otherVertical || val.otherVertical.trim().length < 3)) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["otherVertical"],
      message: "If you select Other, you must specify the vertical (min 3 chars)."
    });
  }
});

const workspaceIdSchema = z.string().uuid();

function validationErrorsToFieldMap(zodError) {
  const out = {};
  for (const issue of zodError.issues ?? []) {
    const key = (issue.path?.[0] ?? "form").toString();
    out[key] = out[key] ? [...out[key], issue.message] : [issue.message];
  }
  return out;
}

function buildAbmBrief(intake) {
  const verticalLabel = intake.verticals.includes("Other") ? intake.otherVertical : intake.verticals.join(", ");
  return {
    icp: {
      segment: intake.segment,
      verticals: verticalLabel,
      geos: intake.geos,
      headcount: intake.targetHeadcountBand,
      salesMotion: intake.salesMotion,
      primaryBuyers: intake.primaryBuyerRoles
    },
    positioning: {
      oneLiner: intake.oneLiner,
      pains: intake.pains,
      differentiation: intake.differentiation,
      competitors: intake.competitors
    },
    channelPlan: [
      "Tier A: LinkedIn outbound + email + exec-to-exec intro + 1:1 landing page",
      "Tier B: LinkedIn + email + vertical webinar/roundtable + retargeting",
      "Tier C: Content syndication + nurture + periodic re-score"
    ],
    measurement: [
      "Coverage: % accounts with contacts in 3+ buying roles",
      "Engagement: reply rate + meeting rate by tier",
      "Pipeline: SQL/SQO + influenced pipeline by tier",
      "Efficiency: cost per meeting + time-to-first-meeting"
    ]
  };
}

function buildScoringTemplate(intake) {
  const weights = {
    firmographic_fit: 30,
    pain_urgency: 25,
    intent_triggers: 25,
    reachability: 20
  };

  const columns = [
    "Account Name","Website/Domain","Geo","Industry/Vertical","Headcount",
    "Revenue Band (optional)","Buying Committee Roles","Current Stack (optional)",
    "Trigger Signals Observed","Firmographic Fit (0-30)","Pain Urgency (0-25)",
    "Intent/Triggers (0-25)","Reachability (0-20)","Total Score (0-100)","Tier (A/B/C)","Notes"
  ];

  const tieringRules = [
    "Tier A: 80–100 (1:1 + exec air cover)",
    "Tier B: 60–79 (1:few by vertical/geo)",
    "Tier C: <60 (nurture/recycle)"
  ];

  const sample = {
    "Account Name": "ExampleCo",
    "Website/Domain": "example.com",
    "Geo": intake.geos[0],
    "Industry/Vertical": intake.verticals.includes("Other") ? intake.otherVertical : intake.verticals[0],
    "Headcount": intake.targetHeadcountBand,
    "Revenue Band (optional)": "",
    "Buying Committee Roles": intake.primaryBuyerRoles.join("; "),
    "Current Stack (optional)": "",
    "Trigger Signals Observed": "Hiring; Website activity",
    "Firmographic Fit (0-30)": 24,
    "Pain Urgency (0-25)": 18,
    "Intent/Triggers (0-25)": 15,
    "Reachability (0-20)": 12,
    "Total Score (0-100)": 69,
    "Tier (A/B/C)": "B",
    "Notes": "Good fit; moderate urgency; build intent."
  };

  const csv = [
    columns.join(","),
    columns.map((c) => JSON.stringify(sample[c] ?? "")).join(",")
  ].join("\n");

  return { weights, columns, tieringRules, sampleRows: [sample], csv };
}

/**
 * Storage:
 * - Local dev default: JSON file
 * - Production: Postgres via DATABASE_URL (Railway-friendly)
 */
const DATA_DIR = "data";
const FILE_DB = join(DATA_DIR, "projects.json");

function ensureFileDb() {
  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
  if (!existsSync(FILE_DB)) writeFileSync(FILE_DB, JSON.stringify({ projects: [] }, null, 2), "utf8");
}

const { Pool } = pg;
const pool = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL }) : null;

async function initPg() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS abm_projects (
      project_id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL,
      client_name TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      intake JSONB NOT NULL,
      outputs JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS abm_projects_workspace_idx ON abm_projects(workspace_id);
  `);
}

async function createProject({ workspaceId, intake, outputs }) {
  const projectId = `proj_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  const record = {
    projectId,
    workspaceId,
    clientName: intake.clientName,
    createdAt: new Date().toISOString(),
    intake,
    outputs
  };

  if (pool) {
    await pool.query(
      `INSERT INTO abm_projects(project_id, workspace_id, client_name, intake, outputs)
       VALUES($1,$2,$3,$4,$5)`,
      [projectId, workspaceId, intake.clientName, intake, outputs]
    );
    return record;
  }

  ensureFileDb();
  const db = JSON.parse(readFileSync(FILE_DB, "utf8"));
  db.projects.unshift(record);
  writeFileSync(FILE_DB, JSON.stringify(db, null, 2), "utf8");
  return record;
}

async function listProjects({ workspaceId, limit = 20 }) {
  if (pool) {
    const { rows } = await pool.query(
      `SELECT project_id, client_name, created_at
       FROM abm_projects
       WHERE workspace_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [workspaceId, limit]
    );
    return rows.map(r => ({ projectId: r.project_id, clientName: r.client_name, createdAt: r.created_at }));
  }

  ensureFileDb();
  const db = JSON.parse(readFileSync(FILE_DB, "utf8"));
  return db.projects
    .filter(p => p.workspaceId === workspaceId)
    .slice(0, limit)
    .map(p => ({ projectId: p.projectId, clientName: p.clientName, createdAt: p.createdAt }));
}

async function getProject({ workspaceId, projectId }) {
  if (pool) {
    const { rows } = await pool.query(
      `SELECT project_id, workspace_id, client_name, created_at, intake, outputs
       FROM abm_projects
       WHERE workspace_id = $1 AND project_id = $2`,
      [workspaceId, projectId]
    );
    if (!rows[0]) return null;
    const r = rows[0];
    return {
      projectId: r.project_id,
      workspaceId: r.workspace_id,
      clientName: r.client_name,
      createdAt: r.created_at,
      intake: r.intake,
      outputs: r.outputs
    };
  }

  ensureFileDb();
  const db = JSON.parse(readFileSync(FILE_DB, "utf8"));
  return db.projects.find(p => p.workspaceId === workspaceId && p.projectId === projectId) ?? null;
}

async function deleteProject({ workspaceId, projectId }) {
  if (pool) {
    const res = await pool.query(
      `DELETE FROM abm_projects WHERE workspace_id = $1 AND project_id = $2`,
      [workspaceId, projectId]
    );
    return res.rowCount > 0;
  }

  ensureFileDb();
  const db = JSON.parse(readFileSync(FILE_DB, "utf8"));
  const before = db.projects.length;
  db.projects = db.projects.filter(p => !(p.workspaceId === workspaceId && p.projectId === projectId));
  writeFileSync(FILE_DB, JSON.stringify(db, null, 2), "utf8");
  return db.projects.length < before;
}

function reply({ message, structuredContent }) {
  return { content: message ? [{ type: "text", text: message }] : [], structuredContent };
}

function createMcp() {
  const mcp = new McpServer({ name: "gtmhelix-abm", version: "0.1.0" });

  // UI widget
  mcp.registerResource("abm-widget", "ui://widget/abm.html", {}, async () => ({
    contents: [{
      uri: "ui://widget/abm.html",
      mimeType: "text/html+skybridge",
      text: widgetHtml,
      _meta: {
        "openai/widgetDescription": "Guided ABM intake + multi-client projects + scoring template.",
        "openai/widgetPrefersBorder": true,
        "openai/widgetCSP": {
          connect_domains: ["https://chatgpt.com"],
          resource_domains: ["https://*.oaistatic.com"]
        }
      }
    }]
  }));

  // Read-only validation
  mcp.registerTool(
    "validate_abm_intake",
    {
      title: "Validate ABM intake",
      description: "Validates intake fields and returns field-level errors if inputs are missing or vague.",
      inputSchema: {
        workspaceId: workspaceIdSchema,
        intake: intakeSchema
      },
      annotations: { readOnlyHint: true }
    },
    async ({ workspaceId, intake }) => {
      const w = workspaceIdSchema.safeParse(workspaceId);
      if (!w.success) return reply({ message: "Invalid workspaceId.", structuredContent: { ok: false, fieldErrors: { workspaceId: ["Must be a UUID."] } } });

      const parsed = intakeSchema.safeParse(intake);
      if (!parsed.success) {
        return reply({
          message: "Fix the highlighted fields before generation.",
          structuredContent: { ok: false, fieldErrors: validationErrorsToFieldMap(parsed.error), draftIntake: intake }
        });
      }
      return reply({ message: "Intake is valid.", structuredContent: { ok: true, fieldErrors: {}, draftIntake: parsed.data } });
    }
  );

  // Write tool: create + save a client project
  mcp.registerTool(
    "create_abm_project",
    {
      title: "Create ABM project",
      description: "Creates a client ABM project: generates ABM brief + scoring template and saves it for later retrieval.",
      inputSchema: {
        workspaceId: workspaceIdSchema,
        intake: intakeSchema
      },
      annotations: { readOnlyHint: false } // write tool => should require confirmation in ChatGPT
    },
    async ({ workspaceId, intake }) => {
      const parsed = intakeSchema.safeParse(intake);
      if (!parsed.success) {
        return reply({
          message: "Cannot create project — intake is incomplete/vague.",
          structuredContent: { ok: false, fieldErrors: validationErrorsToFieldMap(parsed.error), draftIntake: intake }
        });
      }

      const brief = buildAbmBrief(parsed.data);
      const scoring = buildScoringTemplate(parsed.data);
      const outputs = { brief, scoring };

      const record = await createProject({ workspaceId, intake: parsed.data, outputs });

      return reply({
        message: "Project created and saved.",
        structuredContent: {
          ok: true,
          projectId: record.projectId,
          clientName: record.clientName,
          createdAt: record.createdAt,
          abmBrief: brief,
          scoringTemplate: scoring
        }
      });
    }
  );

  // Read-only: list projects
  mcp.registerTool(
    "list_abm_projects",
    {
      title: "List ABM projects",
      description: "Lists saved ABM client projects for this workspace.",
      inputSchema: {
        workspaceId: workspaceIdSchema,
        limit: z.number().int().min(1).max(50).optional()
      },
      annotations: { readOnlyHint: true }
    },
    async ({ workspaceId, limit }) => {
      const projects = await listProjects({ workspaceId, limit: limit ?? 20 });
      return reply({ message: "Saved projects.", structuredContent: { projects } });
    }
  );

  // Read-only: get project
  mcp.registerTool(
    "get_abm_project",
    {
      title: "Get ABM project",
      description: "Fetches a saved ABM project (intake + outputs).",
      inputSchema: {
        workspaceId: workspaceIdSchema,
        projectId: z.string().min(6)
      },
      annotations: { readOnlyHint: true }
    },
    async ({ workspaceId, projectId }) => {
      const proj = await getProject({ workspaceId, projectId });
      if (!proj) return reply({ message: "Not found.", structuredContent: { ok: false } });
      return reply({ message: "Project loaded.", structuredContent: { ok: true, project: proj } });
    }
  );

  // Destructive: delete project
  mcp.registerTool(
    "delete_abm_project",
    {
      title: "Delete ABM project",
      description: "Deletes a saved ABM project.",
      inputSchema: {
        workspaceId: workspaceIdSchema,
        projectId: z.string().min(6)
      },
      annotations: { readOnlyHint: false, destructiveHint: true }
    },
    async ({ workspaceId, projectId }) => {
      const ok = await deleteProject({ workspaceId, projectId });
      return reply({ message: ok ? "Deleted." : "Nothing deleted.", structuredContent: { ok } });
    }
  );

  return mcp;
}

const PORT = Number(process.env.PORT ?? 8787);
const MCP_PATH = "/mcp";

await initPg();

const httpServer = createServer(async (req, res) => {
  if (!req.url) return res.writeHead(400).end("Missing URL");
  const url = new URL(req.url, `http://${req.headers.host ?? "localhost"}`);

  // Basic endpoints
  if (req.method === "GET" && url.pathname === "/") {
    return res.writeHead(200, { "content-type": "text/plain" }).end("GTMHelix ABM MCP server is running.");
  }
  if (req.method === "GET" && url.pathname === "/health") {
    return res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ok: true, db: Boolean(pool) }));
  }

  // CORS (dev)
  if (req.method === "OPTIONS" && url.pathname === MCP_PATH) {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE",
      "Access-Control-Allow-Headers": "content-type, mcp-session-id",
      "Access-Control-Expose-Headers": "Mcp-Session-Id"
    });
    return res.end();
  }

  const MCP_METHODS = new Set(["POST", "GET", "DELETE"]);
  if (url.pathname === MCP_PATH && req.method && MCP_METHODS.has(req.method)) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Expose-Headers", "Mcp-Session-Id");

    const mcp = createMcp();
    const transport = new StreamableHTTPServerTransport({ enableJsonResponse: true });

    res.on("close", () => {
      transport.close();
      mcp.close();
    });

    try {
      await mcp.connect(transport);
      await transport.handleRequest(req, res);
    } catch (e) {
      console.error(e);
      if (!res.headersSent) res.writeHead(500).end("MCP error");
    }
    return;
  }

  res.writeHead(404).end("Not found");
});

httpServer.listen(PORT, () => {
  console.log(`ABM app running: http://localhost:${PORT}${MCP_PATH}`);
});
