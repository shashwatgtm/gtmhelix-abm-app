import "dotenv/config";
import { createServer } from "node:http";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  abmProjectSchema,
  portfolioSchema,
  programSetupSchema,
  validationErrorsToFieldMap,
  buildAbmProjectIntel,
  prioritizePortfolio
} from "./src/abm.js";
import {
  parseCsv,
  mapCsvRowToProject,
  buildBatchOutputCsv
} from "./src/batch.js";
import {
  enrichBusinessOptional,
  mergeExploriumIntoProject
} from "./src/explorium.js";
import { resolveWorkspaceId, initWorkspaceStore } from "./src/workspace.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import pg from "pg";
import { verifyToken } from "@clerk/backend";

/* -----------------------------
   AUTH
------------------------------ */

const AUTH_DISABLED = (process.env.DISABLE_AUTH || "").toLowerCase() === "true";

const CLERK_JWT_KEY = process.env.CLERK_JWT_KEY || "";
const CLERK_SECRET_KEY = process.env.CLERK_SECRET_KEY || "";
const CLERK_ISSUER = (process.env.CLERK_ISSUER || "").replace(/\/+$/, "");
const APP_BASE_URL = (process.env.APP_BASE_URL || "").replace(/\/+$/, "");

const CLERK_AUTHORIZED_PARTIES = (process.env.CLERK_AUTHORIZED_PARTIES || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

/* -----------------------------
   ORIGIN / CORS
------------------------------ */

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS ||
  "https://chatgpt.com,https://chat.openai.com")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function pickCorsOrigin(req) {
  const origin = req.headers.origin;

  if (!origin || typeof origin !== "string") return null;

  if (ALLOWED_ORIGINS.includes("*")) return origin;
  return ALLOWED_ORIGINS.includes(origin) ? origin : false;
}

function setCorsHeaders(req, res) {
  const allowed = pickCorsOrigin(req);

  if (allowed === false) {
    return false;
  }

  if (allowed) {
    res.setHeader("Access-Control-Allow-Origin", allowed);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader(
    "Access-Control-Allow-Headers",
    "content-type, mcp-session-id, mcp-protocol-version, authorization"
  );
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE");
  res.setHeader("Access-Control-Expose-Headers", "Mcp-Session-Id");

  return true;
}

/* -----------------------------
   HELPERS
------------------------------ */

function sendJson(res, status, obj, extraHeaders = {}) {
  res.writeHead(status, { "content-type": "application/json", ...extraHeaders });
  res.end(JSON.stringify(obj));
}

function getBearerToken(req) {
  const raw = req.headers.authorization || req.headers.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

function getExternalBaseUrl(req) {
  if (APP_BASE_URL) return APP_BASE_URL;

  const protoHeader = req.headers["x-forwarded-proto"];
  const hostHeader = req.headers["x-forwarded-host"] || req.headers.host;

  const proto =
    typeof protoHeader === "string" && protoHeader
      ? protoHeader.split(",")[0].trim()
      : "http";

  const host =
    typeof hostHeader === "string" && hostHeader
      ? hostHeader.split(",")[0].trim()
      : "localhost";

  return `${proto}://${host}`;
}

function buildProtectedResourceMetadata(req) {
  const baseUrl = getExternalBaseUrl(req);
  const metadata = {
    resource: `${baseUrl}/mcp`,
    bearer_methods_supported: ["header"],
    resource_documentation: `${baseUrl}/`
  };

  if (CLERK_ISSUER) {
    metadata.authorization_servers = [CLERK_ISSUER];
  }

  return metadata;
}

function buildWwwAuthenticateHeader(req, errorCode) {
  const baseUrl = getExternalBaseUrl(req);
  const params = [
    'Bearer realm="GTMHelix ABM"',
    `resource_metadata="${baseUrl}/.well-known/oauth-protected-resource"`
  ];

  if (errorCode) {
    params.push(`error="${errorCode}"`);
  }

  return params.join(", ");
}

function normalizeWarningText(enrichment) {
  if (!enrichment || typeof enrichment !== "object") return "";

  const warnings = Array.isArray(enrichment.warnings)
    ? enrichment.warnings.filter(Boolean)
    : [];

  if (warnings.length > 0) {
    return warnings.join(" | ");
  }

  if (typeof enrichment.warning === "string" && enrichment.warning.trim()) {
    return enrichment.warning.trim();
  }

  return "";
}

function normalizeFieldsAdded(enrichment) {
  if (!enrichment || typeof enrichment !== "object") return "";
  if (!Array.isArray(enrichment.fieldsAdded)) return "";
  return enrichment.fieldsAdded.filter(Boolean).join(" | ");
}

function buildSkippedEnrichmentResult(reason) {
  return {
    ok: true,
    attempted: false,
    matched: false,
    enrichmentAvailable: false,
    businessId: null,
    reason,
    fieldsAdded: [],
    warnings: [reason]
  };
}

function cleanText(value) {
  return String(value ?? "").trim();
}

function parseBooleanLoose(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const s = cleanText(value).toLowerCase();
  if (["true", "yes", "y", "1"].includes(s)) return true;
  if (["false", "no", "n", "0"].includes(s)) return false;
  return fallback;
}

function uniqueStrings(arr) {
  return [...new Set((arr || []).map((x) => cleanText(x)).filter(Boolean))];
}

function splitListish(value) {
  if (Array.isArray(value)) {
    return uniqueStrings(value);
  }

  const text = cleanText(value);
  if (!text) return [];

  return uniqueStrings(text.split(/[\n,|;]+/g));
}

function pickFirstFilled(...values) {
  for (const value of values) {
    if (Array.isArray(value) && value.length > 0) {
      return value;
    }
    if (cleanText(value)) {
      return value;
    }
  }
  return undefined;
}

function inferObjective(value) {
  const s = cleanText(value).toLowerCase();
  if (!s) return "Net New";
  if (s.includes("expand")) return "Expansion";
  if (s.includes("cross")) return "Cross-Sell";
  if (s.includes("up")) return "Upsell";
  if (s.includes("retain") || s.includes("renew")) return "Retention";
  if (s.includes("new") || s.includes("pipeline") || s.includes("logo")) return "Net New";
  return cleanText(value);
}

function normalizeTargetRolesInput(value) {
  const roles = splitListish(value);
  return roles.length ? roles : ["Economic Buyer", "Technical Buyer", "Champion"];
}

function normalizeIcpList(value, fallback) {
  const items = splitListish(value);
  return items.length ? items : fallback;
}

function buildSuggestedSetupQuestions(draft) {
  const questions = [];

  if (!cleanText(draft.clientName)) {
    questions.push("What company or client is this ABM program for?");
  }
  if (!cleanText(draft.productLine)) {
    questions.push("What product or offering are you selling?");
  }
  if (!Array.isArray(draft.defaultTargetRoles) || draft.defaultTargetRoles.length === 0) {
    questions.push("Which buying roles matter most?");
  }

  return questions;
}

function buildProgramSetupFromConversation(rawArgs) {
  const source =
    rawArgs?.brief && typeof rawArgs.brief === "object"
      ? rawArgs.brief
      : rawArgs || {};

  const appliedDefaults = [];

  const clientName = cleanText(
    pickFirstFilled(
      source.clientName,
      source.client,
      source.companyName,
      source.brandName
    )
  );

  const productLine = cleanText(
    pickFirstFilled(
      source.productLine,
      source.whatYouSell,
      source.offering,
      source.solution,
      source.product
    )
  );

  const objectiveInput = pickFirstFilled(
    source.objective,
    source.goal,
    source.primaryGoal,
    source.useCase
  );
  const objective = inferObjective(objectiveInput);
  if (!cleanText(objectiveInput)) {
    appliedDefaults.push('objective="Net New"');
  }

  const defaultTargetRoles = normalizeTargetRolesInput(
    pickFirstFilled(
      source.defaultTargetRoles,
      source.targetRoles,
      source.buyingRoles,
      source.roles
    )
  );
  if (
    !pickFirstFilled(
      source.defaultTargetRoles,
      source.targetRoles,
      source.buyingRoles,
      source.roles
    )
  ) {
    appliedDefaults.push(
      'defaultTargetRoles=["Economic Buyer","Technical Buyer","Champion"]'
    );
  }

  const preferredVerticals = normalizeIcpList(
    pickFirstFilled(
      source.preferredVerticals,
      source.verticals,
      source.icpVerticals
    ),
    ["Technology"]
  );
  if (
    !pickFirstFilled(
      source.preferredVerticals,
      source.verticals,
      source.icpVerticals
    )
  ) {
    appliedDefaults.push('preferredVerticals=["Technology"]');
  }

  const preferredRegions = normalizeIcpList(
    pickFirstFilled(
      source.preferredRegions,
      source.regions,
      source.icpRegions
    ),
    ["North America"]
  );
  if (
    !pickFirstFilled(
      source.preferredRegions,
      source.regions,
      source.icpRegions
    )
  ) {
    appliedDefaults.push('preferredRegions=["North America"]');
  }

  const preferredSegments = normalizeIcpList(
    pickFirstFilled(
      source.preferredSegments,
      source.segments,
      source.icpSegments
    ),
    ["Mid-Market", "Enterprise"]
  );
  if (
    !pickFirstFilled(
      source.preferredSegments,
      source.segments,
      source.icpSegments
    )
  ) {
    appliedDefaults.push('preferredSegments=["Mid-Market","Enterprise"]');
  }

  const preferredMotions = normalizeIcpList(
    pickFirstFilled(
      source.preferredMotions,
      source.motions,
      source.gtmMotions
    ),
    ["Sales-Led", "Hybrid"]
  );
  if (
    !pickFirstFilled(
      source.preferredMotions,
      source.motions,
      source.gtmMotions
    )
  ) {
    appliedDefaults.push('preferredMotions=["Sales-Led","Hybrid"]');
  }

  const ownerName = cleanText(
    pickFirstFilled(
      source.ownerName,
      source.executionOwner,
      source.sellerName,
      source.owner
    )
  ) || "Program Owner";
  if (
    !pickFirstFilled(
      source.ownerName,
      source.executionOwner,
      source.sellerName,
      source.owner
    )
  ) {
    appliedDefaults.push('sellerContext.ownerName="Program Owner"');
  }

  const ownerRole = cleanText(
    pickFirstFilled(
      source.ownerRole,
      source.executionOwnerRole,
      source.sellerRole
    )
  );

  const motionOwner = cleanText(
    pickFirstFilled(
      source.motionOwner,
      source.executionTeam,
      source.executionFunction
    )
  );

  const channelsAvailable = normalizeIcpList(
    pickFirstFilled(
      source.channelsAvailable,
      source.channels,
      source.outreachChannels
    ),
    ["Email", "LinkedIn"]
  );
  if (
    !pickFirstFilled(
      source.channelsAvailable,
      source.channels,
      source.outreachChannels
    )
  ) {
    appliedDefaults.push('sellerContext.channelsAvailable=["Email","LinkedIn"]');
  }

  const requiredTech = splitListish(
    pickFirstFilled(source.requiredTech, source.techRequirements)
  );
  const excludedVerticals = splitListish(source.excludedVerticals);
  const excludedRegions = splitListish(source.excludedRegions);
  const excludedSignals = splitListish(source.excludedSignals);

  const promotionTriggerDefinition =
    cleanText(source.promotionTriggerDefinition) ||
    "Promote when fit, demand, and committee evidence justify higher investment.";

  const enrichmentAllowed = parseBooleanLoose(source.enrichmentAllowed, true);
  if (source.enrichmentAllowed === undefined) {
    appliedDefaults.push("enrichmentAllowed=true");
  }

  const candidateSetup = {
    clientName,
    objective,
    productLine,
    defaultTargetRoles,
    defaultICP: {
      preferredVerticals,
      preferredRegions,
      preferredSegments,
      preferredMotions,
      requiredTech,
      excludedVerticals,
      excludedRegions,
      excludedSignals
    },
    sellerContext: {
      ownerName,
      ownerRole: ownerRole || undefined,
      motionOwner: motionOwner || undefined,
      channelsAvailable,
      promotionTriggerDefinition
    },
    enrichmentAllowed
  };

  return {
    candidateSetup,
    appliedDefaults,
    suggestedQuestions: buildSuggestedSetupQuestions(candidateSetup)
  };
}

/* -----------------------------
   AUTH VALIDATION
------------------------------ */

async function requireAuth(req, res) {
  if (AUTH_DISABLED) {
    return {
      userId: "dev_user",
      orgId: "dev_org",
      tenantId: "dev_org",
      claims: { sub: "dev_user", org_id: "dev_org" }
    };
  }

  if (!CLERK_JWT_KEY && !CLERK_SECRET_KEY) {
    sendJson(res, 500, {
      error: "AuthMisconfigured",
      message:
        "Set CLERK_JWT_KEY (preferred) or CLERK_SECRET_KEY. Or set DISABLE_AUTH=true for local dev."
    });
    return null;
  }

  const token = getBearerToken(req);

  if (!token) {
    sendJson(
      res,
      401,
      {
        error: "Unauthorized",
        message: "Missing Authorization header. Send: Authorization: Bearer <JWT>"
      },
      { "WWW-Authenticate": buildWwwAuthenticateHeader(req, "invalid_token") }
    );
    return null;
  }

  try {
    const verification = await verifyToken(token, {
      jwtKey: CLERK_JWT_KEY || undefined,
      secretKey: CLERK_SECRET_KEY || undefined,
      authorizedParties:
        CLERK_AUTHORIZED_PARTIES.length ? CLERK_AUTHORIZED_PARTIES : undefined
    });

    const claims = verification?.data ?? verification;
    if (!claims || typeof claims !== "object" || !claims.sub) {
      sendJson(
        res,
        401,
        { error: "Unauthorized", message: "Invalid token." },
        { "WWW-Authenticate": buildWwwAuthenticateHeader(req, "invalid_token") }
      );
      return null;
    }

    const orgId = claims.org_id || null;
    const userId = claims.sub;
    const tenantId = orgId || userId;

    return { userId, orgId, tenantId, claims };
  } catch {
    sendJson(
      res,
      401,
      { error: "Unauthorized", message: "Token verification failed." },
      { "WWW-Authenticate": buildWwwAuthenticateHeader(req, "invalid_token") }
    );
    return null;
  }
}

/* -----------------------------
   STORAGE
------------------------------ */

const widgetHtml = readFileSync("public/abm-widget.html", "utf8");

const DATA_DIR = "data";
const FILE_DB = join(DATA_DIR, "projects.json");

function ensureFileDb() {
  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
  if (!existsSync(FILE_DB)) {
    writeFileSync(FILE_DB, JSON.stringify({ projects: [] }, null, 2), "utf8");
  }
}

const { Pool } = pg;
const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL })
  : null;

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
    CREATE INDEX IF NOT EXISTS abm_projects_workspace_idx
      ON abm_projects(workspace_id);

    CREATE TABLE IF NOT EXISTS abm_workspaces (
      tenant_id TEXT PRIMARY KEY,
      workspace_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
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

    return rows.map((r) => ({
      projectId: r.project_id,
      clientName: r.client_name,
      createdAt: r.created_at
    }));
  }

  ensureFileDb();
  const db = JSON.parse(readFileSync(FILE_DB, "utf8"));
  return db.projects
    .filter((p) => p.workspaceId === workspaceId)
    .slice(0, limit)
    .map((p) => ({
      projectId: p.projectId,
      clientName: p.clientName,
      createdAt: p.createdAt
    }));
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
  return (
    db.projects.find(
      (p) => p.workspaceId === workspaceId && p.projectId === projectId
    ) ?? null
  );
}

async function deleteProject({ workspaceId, projectId }) {
  if (pool) {
    const result = await pool.query(
      `DELETE FROM abm_projects WHERE workspace_id = $1 AND project_id = $2`,
      [workspaceId, projectId]
    );
    return result.rowCount > 0;
  }

  ensureFileDb();
  const db = JSON.parse(readFileSync(FILE_DB, "utf8"));
  const before = db.projects.length;
  db.projects = db.projects.filter(
    (p) => !(p.workspaceId === workspaceId && p.projectId === projectId)
  );
  writeFileSync(FILE_DB, JSON.stringify(db, null, 2), "utf8");
  return db.projects.length < before;
}

function reply({ message, structuredContent }) {
  return {
    content: message ? [{ type: "text", text: message }] : [],
    structuredContent
  };
}

/* -----------------------------
   ARG NORMALIZATION
------------------------------ */

function normalizeProjectArgs(raw) {
  if (!raw || typeof raw !== "object") {
    return { project: null };
  }

  if (raw.project && typeof raw.project === "object") {
    return { project: raw.project };
  }

  if (raw.clientName) {
    return { project: raw };
  }

  return { project: null };
}

function normalizePortfolioArgs(raw) {
  if (!raw || typeof raw !== "object") {
    return { portfolio: null };
  }

  if (raw.portfolio && typeof raw.portfolio === "object") {
    return { portfolio: raw.portfolio };
  }

  if (Array.isArray(raw.accounts)) {
    return { portfolio: raw };
  }

  return { portfolio: null };
}

/* -----------------------------
   ENRICHMENT HELPERS
------------------------------ */

async function enrichProjectOptionally(project, enrichmentAllowed = true) {
  if (!enrichmentAllowed) {
    const enrichment = buildSkippedEnrichmentResult(
      "Explorium enrichment skipped by policy."
    );

    return {
      enrichment,
      mergedProject: project
    };
  }

  const enrichment = await enrichBusinessOptional({
    accountName: project.account.accountName,
    domain: project.account.domain,
    linkedinUrl: undefined
  });

  const mergedProject = mergeExploriumIntoProject(project, enrichment);

  if (
    enrichment &&
    enrichment.matched === true &&
    (!Array.isArray(enrichment.fieldsAdded) || enrichment.fieldsAdded.length === 0) &&
    (!enrichment.reason || typeof enrichment.reason !== "string")
  ) {
    enrichment.reason =
      "Matched business, but no project fields were updated because existing values already existed.";
  }

  return { enrichment, mergedProject };
}

/* -----------------------------
   MCP
------------------------------ */

async function createMcp(auth) {
  const mcp = new McpServer({ name: "gtmhelix-abm", version: "1.3.0" });

  await initWorkspaceStore({ pool });
  const workspaceId = await resolveWorkspaceId({ tenantId: auth.tenantId, pool });

  mcp.registerResource("abm-widget", "ui://widget/abm.html", {}, async () => ({
    contents: [
      {
        uri: "ui://widget/abm.html",
        mimeType: "text/html+skybridge",
        text: widgetHtml,
        _meta: {
          "openai/widgetDescription":
            "B2B ABM intelligence engine: capture setup defaults once, score single accounts or CSV batches, optionally enrich with Explorium, and generate orchestration-ready outputs.",
          "openai/widgetPrefersBorder": true,
          "openai/widgetCSP": {
            connect_domains: [
              "https://chatgpt.com",
              "https://chat.openai.com",
              "https://*.railway.app"
            ],
            resource_domains: ["https://*.oaistatic.com"]
          }
        }
      }
    ]
  }));

  mcp.registerTool(
    "whoami",
    {
      title: "Who am I?",
      description:
        "Use this when you need the current authenticated tenant context and canonical workspace ID.",
      inputSchema: z.object({}).passthrough(),
      annotations: { readOnlyHint: true }
    },
    async () => {
      return reply({
        message: "Auth context.",
        structuredContent: { ok: true, userId: auth.userId, orgId: auth.orgId, workspaceId }
      });
    }
  );

  mcp.registerTool(
    "setup_abm_program",
    {
      title: "Setup ABM program defaults",
      description:
        "Use this after asking the user a few setup questions once for the whole batch. It accepts either the full strict setup schema or a simpler conversational payload like whatYouSell, goal, roles, verticals, regions, segments, motions, executionOwner, channels, and enrichmentAllowed. It returns normalized defaults that can be reused for single-account or CSV batch scoring.",
      inputSchema: z.any(),
      annotations: { readOnlyHint: true }
    },
    async (rawArgs) => {
      const strictParsed = programSetupSchema.safeParse(rawArgs);

      if (strictParsed.success) {
        return reply({
          message: "Program defaults captured.",
          structuredContent: {
            ok: true,
            workspaceId,
            inputMode: "strict",
            setup: strictParsed.data,
            appliedDefaults: [],
            nextStep:
              "Ask the user to paste CSV content or upload a sheet and then use score_abm_csv_batch with these defaults."
          }
        });
      }

      const { candidateSetup, appliedDefaults, suggestedQuestions } =
        buildProgramSetupFromConversation(rawArgs);

      const conversationalParsed = programSetupSchema.safeParse(candidateSetup);

      if (!conversationalParsed.success) {
        return reply({
          message: "I still need a few setup details before I can lock the ABM defaults.",
          structuredContent: {
            ok: false,
            inputMode: "conversational",
            fieldErrors: validationErrorsToFieldMap(conversationalParsed.error),
            draftSetup: candidateSetup,
            appliedDefaults,
            suggestedQuestions
          }
        });
      }

      return reply({
        message: "Program defaults captured from conversational input.",
        structuredContent: {
          ok: true,
          workspaceId,
          inputMode: "conversational",
          setup: conversationalParsed.data,
          appliedDefaults,
          nextStep:
            "Ask the user to paste CSV content or upload a sheet and then use score_abm_csv_batch with these defaults."
        }
      });
    }
  );

  mcp.registerTool(
    "enrich_account_with_explorium",
    {
      title: "Enrich account with Explorium",
      description:
        "Optionally enrich a B2B account using Explorium. Use when account name and optional domain are available. Never assume enrichment is required.",
      inputSchema: z
        .object({
          accountName: z.string().min(2),
          domain: z.string().optional(),
          linkedinUrl: z.string().optional()
        })
        .passthrough(),
      annotations: { readOnlyHint: true }
    },
    async ({ accountName, domain, linkedinUrl }) => {
      const enrichment = await enrichBusinessOptional({
        accountName,
        domain,
        linkedinUrl
      });

      return reply({
        message:
          enrichment.matched === true
            ? "Explorium enrichment completed."
            : enrichment.attempted === true
              ? "Explorium enrichment attempted but no usable enrichment was applied."
              : "Explorium enrichment skipped or unavailable.",
        structuredContent: {
          ok: true,
          workspaceId,
          enrichment
        }
      });
    }
  );

  mcp.registerTool(
    "score_abm_account",
    {
      title: "Score ABM account",
      description:
        "Use this to compute ABM intelligence for a single B2B account. Use program setup defaults where available. Optional Explorium enrichment improves factual completeness but is never required.",
      inputSchema: z.any(),
      annotations: { readOnlyHint: true }
    },
    async (rawArgs) => {
      const { project } = normalizeProjectArgs(rawArgs);
      if (!project) {
        return reply({
          message: 'Invalid input shape. Pass JSON like: { "project": { ... } }',
          structuredContent: {
            ok: false,
            fieldErrors: { form: ["Missing project object."] },
            draftProject: rawArgs
          }
        });
      }

      const parsed = abmProjectSchema.safeParse(project);
      if (!parsed.success) {
        return reply({
          message: "Project input is invalid.",
          structuredContent: {
            ok: false,
            fieldErrors: validationErrorsToFieldMap(parsed.error),
            draftProject: project
          }
        });
      }

      const enrichmentAllowed =
        parsed.data.programDefaults?.enrichmentAllowed !== false;

      const { enrichment, mergedProject } = await enrichProjectOptionally(
        parsed.data,
        enrichmentAllowed
      );
      const intel = buildAbmProjectIntel(mergedProject);

      return reply({
        message: "Account scored.",
        structuredContent: {
          ok: true,
          workspaceId,
          clientName: mergedProject.clientName,
          enrichment,
          intelligence: intel
        }
      });
    }
  );

  mcp.registerTool(
    "score_abm_csv_batch",
    {
      title: "Score ABM CSV batch",
      description:
        "Use this after setup_abm_program. Accepts CSV text plus batch defaults, scores every row, optionally enriches missing factual data with Explorium, and returns both structured results and an orchestration-ready CSV output. This is the main tool for 25-1000 account ABM workflows.",
      inputSchema: z
        .object({
          defaults: z.any(),
          csvText: z.string().min(10),
          enrichMissingFacts: z.boolean().optional().default(true),
          topN: z.number().int().min(1).max(1000).optional().default(50)
        })
        .passthrough(),
      annotations: { readOnlyHint: true }
    },
    async ({ defaults, csvText, enrichMissingFacts, topN }) => {
      const setupParsed = programSetupSchema.safeParse(defaults);
      if (!setupParsed.success) {
        return reply({
          message: "Batch defaults are invalid.",
          structuredContent: {
            ok: false,
            fieldErrors: validationErrorsToFieldMap(setupParsed.error),
            draftSetup: defaults
          }
        });
      }

      let rows;
      try {
        rows = parseCsv(csvText);
      } catch (e) {
        return reply({
          message: "CSV could not be parsed.",
          structuredContent: {
            ok: false,
            fieldErrors: { csvText: [e.message] }
          }
        });
      }

      const scoredRows = [];
      const rowErrors = [];

      for (let i = 0; i < rows.length; i += 1) {
        const row = rows[i];
        const projectCandidate = mapCsvRowToProject(row, setupParsed.data);
        const parsed = abmProjectSchema.safeParse(projectCandidate);

        if (!parsed.success) {
          rowErrors.push({
            rowNumber: i + 2,
            accountName: row.account_name || row.accountName || "",
            errors: validationErrorsToFieldMap(parsed.error)
          });
          continue;
        }

        const allowEnrichment =
          setupParsed.data.enrichmentAllowed !== false &&
          enrichMissingFacts === true;

        const { enrichment, mergedProject } = await enrichProjectOptionally(
          parsed.data,
          allowEnrichment
        );
        const intelligence = buildAbmProjectIntel(mergedProject);

        scoredRows.push({
          rowNumber: i + 2,
          clientName: mergedProject.clientName,
          accountName: mergedProject.account.accountName,
          accountDomain: mergedProject.account.domain || "",
          vertical: mergedProject.account.vertical,
          region: mergedProject.account.region,
          segment: mergedProject.account.segment,
          objective: mergedProject.objective,
          productLine: mergedProject.productLine,
          totalScore: intelligence.totalScore,
          tier: intelligence.tier,
          buyingStage: intelligence.buyingStage,
          stageConfidence: intelligence.stageConfidence,
          fitScore: intelligence.scores.fit,
          intentScore: intelligence.scores.intent,
          committeeCoverageScore: intelligence.scores.committeeCoverage,
          relationshipScore: intelligence.scores.relationship,
          overallConfidence: intelligence.confidence.overallConfidence,
          ownerAction:
            intelligence.tier === "Tier 1"
              ? "Assign AE/owner to 1:1 plan"
              : intelligence.tier === "Tier 2"
                ? "Run tailored 1:few motion"
                : "Keep in monitored nurture",
          minimumNextStep: intelligence.executionPlan[0] || "",
          promotionCriteria:
            intelligence.tier === "Tier 3"
              ? "Promote when stakeholder access or credible demand signal appears"
              : "Advance when buying-stage evidence and committee coverage improve",
          doNotDo:
            intelligence.tier === "Tier 3"
              ? "Do not overinvest in bespoke content or heavy paid spend"
              : "Do not skip committee mapping",
          messageHypothesis: intelligence.plays[0] || "",
          recommendedPlays: intelligence.plays.join(" | "),
          executionPlan: intelligence.executionPlan.join(" | "),
          watchouts: intelligence.watchouts.join(" | "),
          missingRoles: intelligence.explainability.missingRoles.join(" | "),
          missingDataWarnings: intelligence.explainability.missingDataWarnings.join(" | "),
          exploriumAttempted: enrichment?.attempted === true ? "true" : "false",
          exploriumMatched: enrichment?.matched === true ? "true" : "false",
          exploriumBusinessId: enrichment?.businessId || "",
          exploriumReason: enrichment?.reason || "",
          exploriumFieldsAdded: normalizeFieldsAdded(enrichment),
          exploriumWarning: normalizeWarningText(enrichment),
          fitReasons: intelligence.explainability.fitReasons.join(" | "),
          fitRisks: intelligence.explainability.fitRisks.join(" | "),
          topSignals: intelligence.explainability.topSignals.join(" | "),
          coveredRoles: intelligence.explainability.coveredRoles.join(" | "),
          penalties: intelligence.explainability.penalties.join(" | ")
        });
      }

      const outputCsv = buildBatchOutputCsv(scoredRows);

      const prioritized = {
        summary: {
          "Tier 1": scoredRows.filter((r) => r.tier === "Tier 1").length,
          "Tier 2": scoredRows.filter((r) => r.tier === "Tier 2").length,
          "Tier 3": scoredRows.filter((r) => r.tier === "Tier 3").length
        }
      };

      return reply({
        message: "CSV batch scored.",
        structuredContent: {
          ok: true,
          workspaceId,
          summary: {
            totalRows: rows.length,
            scoredRows: scoredRows.length,
            errorRows: rowErrors.length,
            tier1: scoredRows.filter((r) => r.tier === "Tier 1").length,
            tier2: scoredRows.filter((r) => r.tier === "Tier 2").length,
            tier3: scoredRows.filter((r) => r.tier === "Tier 3").length,
            enrichmentAttemptedRows: scoredRows.filter(
              (r) => r.exploriumAttempted === "true"
            ).length,
            enrichmentMatchedRows: scoredRows.filter(
              (r) => r.exploriumMatched === "true"
            ).length,
            enrichmentFieldsAddedRows: scoredRows.filter(
              (r) =>
                typeof r.exploriumFieldsAdded === "string" &&
                r.exploriumFieldsAdded.length > 0
            ).length
          },
          rowErrors,
          topAccounts: scoredRows
            .slice()
            .sort((a, b) => b.totalScore - a.totalScore)
            .slice(0, Math.min(topN, scoredRows.length)),
          outputCsv,
          portfolioSummary: prioritized.summary
        }
      });
    }
  );

  mcp.registerTool(
    "prioritize_abm_portfolio",
    {
      title: "Prioritize ABM portfolio",
      description:
        "Use this to score and rank multiple target accounts, assign ABM tiers, and output a prioritized portfolio with reasons and next plays.",
      inputSchema: z.any(),
      annotations: { readOnlyHint: true }
    },
    async (rawArgs) => {
      const { portfolio } = normalizePortfolioArgs(rawArgs);
      if (!portfolio) {
        return reply({
          message: 'Invalid input shape. Pass JSON like: { "portfolio": { "accounts": [...] } }',
          structuredContent: {
            ok: false,
            fieldErrors: { form: ["Missing portfolio object."] },
            draftPortfolio: rawArgs
          }
        });
      }

      const parsed = portfolioSchema.safeParse(portfolio);
      if (!parsed.success) {
        return reply({
          message: "Portfolio input is invalid.",
          structuredContent: {
            ok: false,
            fieldErrors: validationErrorsToFieldMap(parsed.error),
            draftPortfolio: portfolio
          }
        });
      }

      const prioritized = prioritizePortfolio(parsed.data);

      return reply({
        message: "Portfolio prioritized.",
        structuredContent: {
          ok: true,
          workspaceId,
          portfolio: prioritized
        }
      });
    }
  );

  mcp.registerTool(
    "create_abm_project",
    {
      title: "Create ABM project",
      description:
        "Use this to save a scored ABM account strategy project with explainable scoring, plays, committee gaps, execution recommendations, and optional Explorium enrichment.",
      inputSchema: z.any(),
      annotations: { readOnlyHint: false }
    },
    async (rawArgs) => {
      const { project } = normalizeProjectArgs(rawArgs);
      if (!project) {
        return reply({
          message: 'Invalid input shape. Pass JSON like: { "project": { ... } }',
          structuredContent: {
            ok: false,
            fieldErrors: { form: ["Missing project object."] },
            draftProject: rawArgs
          }
        });
      }

      const parsed = abmProjectSchema.safeParse(project);
      if (!parsed.success) {
        return reply({
          message: "Cannot create project because the input is invalid.",
          structuredContent: {
            ok: false,
            fieldErrors: validationErrorsToFieldMap(parsed.error),
            draftProject: project
          }
        });
      }

      const enrichmentAllowed =
        parsed.data.programDefaults?.enrichmentAllowed !== false;

      const { enrichment, mergedProject } = await enrichProjectOptionally(
        parsed.data,
        enrichmentAllowed
      );
      const intel = buildAbmProjectIntel(mergedProject);
      const outputs = { enrichment, intelligence: intel };

      const record = await createProject({
        workspaceId,
        intake: mergedProject,
        outputs
      });

      return reply({
        message: "Project created and saved.",
        structuredContent: {
          ok: true,
          workspaceId,
          projectId: record.projectId,
          clientName: record.clientName,
          createdAt: record.createdAt,
          enrichment,
          intelligence: intel
        }
      });
    }
  );

  mcp.registerTool(
    "list_abm_projects",
    {
      title: "List ABM projects",
      description:
        "Use this to list saved ABM account strategy projects in the current workspace.",
      inputSchema: z
        .object({ limit: z.number().int().min(1).max(50).optional() })
        .passthrough(),
      annotations: { readOnlyHint: true }
    },
    async ({ limit }) => {
      const projects = await listProjects({ workspaceId, limit: limit ?? 20 });
      return reply({
        message: "Saved projects.",
        structuredContent: { workspaceId, projects }
      });
    }
  );

  mcp.registerTool(
    "get_abm_project",
    {
      title: "Get ABM project",
      description:
        "Use this to load a saved ABM project with full scored intelligence and enrichment output if available.",
      inputSchema: z.object({ projectId: z.string().min(6) }).passthrough(),
      annotations: { readOnlyHint: true }
    },
    async ({ projectId }) => {
      const proj = await getProject({ workspaceId, projectId });
      if (!proj) {
        return reply({ message: "Not found.", structuredContent: { ok: false } });
      }
      return reply({
        message: "Project loaded.",
        structuredContent: { ok: true, workspaceId, project: proj }
      });
    }
  );

  mcp.registerTool(
    "delete_abm_project",
    {
      title: "Delete ABM project",
      description: "Use this to delete a saved ABM project.",
      inputSchema: z.object({ projectId: z.string().min(6) }).passthrough(),
      annotations: { readOnlyHint: false, destructiveHint: true }
    },
    async ({ projectId }) => {
      const ok = await deleteProject({ workspaceId, projectId });
      return reply({
        message: ok ? "Deleted." : "Nothing deleted.",
        structuredContent: { ok, workspaceId }
      });
    }
  );

  return mcp;
}

/* -----------------------------
   HTTP SERVER
------------------------------ */

const PORT = Number(process.env.PORT ?? 8787);
const MCP_PATH = "/mcp";

await initPg();

const httpServer = createServer(async (req, res) => {
  if (!req.url) return res.writeHead(400).end("Missing URL");

  const url = new URL(req.url, `http://${req.headers.host ?? "localhost"}`);

  if (!setCorsHeaders(req, res)) {
    return res.writeHead(403).end("Origin not allowed.");
  }

  if (req.method === "OPTIONS" && url.pathname === MCP_PATH) {
    res.writeHead(204);
    return res.end();
  }

  if (req.method === "GET" && url.pathname === "/") {
    return res
      .writeHead(200, { "content-type": "text/plain" })
      .end("GTMHelix ABM MCP server is running.");
  }

  if (req.method === "GET" && url.pathname === "/health") {
    return res.writeHead(200, { "content-type": "application/json" }).end(
      JSON.stringify({
        ok: true,
        db: Boolean(pool),
        authDisabled: AUTH_DISABLED,
        allowedOrigins: ALLOWED_ORIGINS,
        exploriumEnabled:
          (process.env.EXPLORIUM_ENABLED || "").toLowerCase() === "true"
      })
    );
  }

  if (
    req.method === "GET" &&
    url.pathname === "/.well-known/oauth-protected-resource"
  ) {
    return sendJson(res, 200, buildProtectedResourceMetadata(req));
  }

  const MCP_METHODS = new Set(["POST", "GET", "DELETE"]);
  if (url.pathname === MCP_PATH && req.method && MCP_METHODS.has(req.method)) {
    const auth = await requireAuth(req, res);
    if (!auth) return;

    const mcp = await createMcp(auth);
    const transport = new StreamableHTTPServerTransport({
      enableJsonResponse: true
    });

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
  console.log(
    AUTH_DISABLED
      ? "AUTH: DISABLED (DISABLE_AUTH=true)"
      : "AUTH: ENABLED (expects Authorization: Bearer <Clerk session JWT>)"
  );
  console.log(
    CLERK_ISSUER
      ? `OAUTH METADATA: ENABLED via ${CLERK_ISSUER}`
      : "OAUTH METADATA: PARTIAL - set CLERK_ISSUER for authorization_servers metadata"
  );
  console.log(
    (process.env.EXPLORIUM_ENABLED || "").toLowerCase() === "true"
      ? "EXPLORIUM: ENABLED (optional enrichment)"
      : "EXPLORIUM: DISABLED"
  );
});