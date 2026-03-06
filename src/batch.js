function normalizeHeader(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
}

function splitLines(text) {
  return String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .filter((line) => line.trim().length > 0);
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }

    cur += ch;
  }

  out.push(cur);
  return out.map((v) => v.trim());
}

export function parseCsv(text) {
  const lines = splitLines(text);
  if (!lines.length) {
    throw new Error("CSV is empty.");
  }

  const headers = parseCsvLine(lines[0]).map(normalizeHeader);
  const rows = [];

  for (let i = 1; i < lines.length; i += 1) {
    const values = parseCsvLine(lines[i]);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] ?? "";
    });
    rows.push(row);
  }

  return rows;
}

function parseBool(v) {
  const s = String(v || "").trim().toLowerCase();
  if (!s) return undefined;
  if (["true", "yes", "y", "1"].includes(s)) return true;
  if (["false", "no", "n", "0"].includes(s)) return false;
  return undefined;
}

function parseNum(v) {
  if (v === undefined || v === null || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function splitPipe(v) {
  return String(v || "")
    .split("|")
    .map((s) => s.trim())
    .filter(Boolean);
}

function parseSignals(v) {
  return splitPipe(v).map((item) => {
    const [signalType, strength, recencyDays] = item.split(":").map((s) => s.trim());
    return {
      signalType,
      strength: Number(strength || 0),
      recencyDays: Number(recencyDays || 30)
    };
  });
}

function parseCommittee(v) {
  return splitPipe(v).map((item) => {
    const [name, title, role, relationshipStrength, engaged] = item.split(";").map((s) => s.trim());
    return {
      name,
      title,
      role,
      relationshipStrength: Number(relationshipStrength || 0),
      engaged: ["true", "yes", "1"].includes(String(engaged || "").toLowerCase())
    };
  });
}

function pick(row, key, fallback) {
  const v = row[key];
  return v !== undefined && v !== null && String(v).trim() !== "" ? String(v).trim() : fallback;
}

function toArrayOrDefault(v, fallback) {
  const arr = splitPipe(v);
  return arr.length ? arr : fallback;
}

export function mapCsvRowToProject(row, defaults) {
  const clientName = pick(row, "client_name", defaults.clientName);
  const objective = pick(row, "objective", defaults.objective);
  const productLine = pick(row, "product_line", defaults.productLine);
  const targetRoles = toArrayOrDefault(row.target_roles, defaults.defaultTargetRoles);

  const vertical = pick(
    row,
    "vertical",
    defaults.defaultICP?.preferredVerticals?.[0]
  );

  const region = pick(
    row,
    "region",
    defaults.defaultICP?.preferredRegions?.[0]
  );

  const segment = pick(
    row,
    "segment",
    defaults.defaultICP?.preferredSegments?.[0]
  );

  const gtmMotion = pick(
    row,
    "gtm_motion",
    defaults.defaultICP?.preferredMotions?.[0] || "Sales-Led"
  );

  return {
    clientName,
    objective,
    productLine,
    targetRoles,
    sellerContext: defaults.sellerContext || {},
    programDefaults: {
      enrichmentAllowed: defaults.enrichmentAllowed !== false
    },
    icp: {
      preferredVerticals: defaults.defaultICP?.preferredVerticals || [],
      preferredRegions: defaults.defaultICP?.preferredRegions || [],
      preferredSegments: defaults.defaultICP?.preferredSegments || [],
      preferredMotions: defaults.defaultICP?.preferredMotions || [],
      minEmployeeBand: defaults.defaultICP?.minEmployeeBand,
      minSecurityMaturity: defaults.defaultICP?.minSecurityMaturity,
      requiredTech: defaults.defaultICP?.requiredTech || [],
      excludedVerticals: defaults.defaultICP?.excludedVerticals || [],
      excludedRegions: defaults.defaultICP?.excludedRegions || [],
      excludedSignals: defaults.defaultICP?.excludedSignals || []
    },
    account: {
      accountName: pick(row, "account_name", ""),
      domain: pick(row, "account_domain", "unknown"),
      vertical,
      region,
      segment,
      gtmMotion,
      employeeBand: pick(row, "employee_band", undefined),
      revenueBand: pick(row, "revenue_band", undefined),
      accountCoverageModel: pick(row, "account_coverage_model", "Named"),
      existingCustomer: parseBool(row.existing_customer),
      expansionCandidate: parseBool(row.expansion_candidate),
      openOpportunity: parseBool(row.open_opportunity),
      strategicNamed: parseBool(row.strategic_named),
      competitorPresent: parseBool(row.competitor_present),
      partnerAvailable: parseBool(row.partner_available),
      currentPipelineStage: pick(row, "current_pipeline_stage", "No Opportunity"),
      dealPotentialBand: pick(row, "deal_potential_band", "Medium"),
      firstPartyEngagementScore: parseNum(row.first_party_engagement_score),
      thirdPartyIntentScore: parseNum(row.third_party_intent_score),
      relationshipScore: parseNum(row.relationship_score),
      notes: pick(row, "notes", ""),
      signals: parseSignals(row.signals),
      committee: parseCommittee(row.committee_members),
      tech: {
        crm: pick(row, "tech_crm", "Unknown"),
        map: pick(row, "tech_map", "Unknown"),
        dataWarehouse: pick(row, "tech_data_warehouse", "Unknown"),
        cloud: pick(row, "tech_cloud", "Unknown"),
        securityMaturity: pick(row, "tech_security_maturity", "Moderate"),
        stackFitScore: parseNum(row.tech_stack_fit_score) ?? 50
      }
    }
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export function buildBatchOutputCsv(rows) {
  if (!rows.length) return "";

  const headers = [
    "clientName",
    "accountName",
    "accountDomain",
    "vertical",
    "region",
    "segment",
    "objective",
    "productLine",
    "totalScore",
    "tier",
    "buyingStage",
    "stageConfidence",
    "fitScore",
    "intentScore",
    "committeeCoverageScore",
    "relationshipScore",
    "overallConfidence",
    "ownerAction",
    "minimumNextStep",
    "promotionCriteria",
    "doNotDo",
    "messageHypothesis",
    "recommendedPlays",
    "executionPlan",
    "watchouts",
    "missingRoles",
    "missingDataWarnings",
    "exploriumMatched",
    "exploriumBusinessId",
    "exploriumWarning",
    "fitReasons",
    "fitRisks",
    "topSignals",
    "coveredRoles",
    "penalties"
  ];

  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((h) => csvEscape(row[h])).join(","))
  ];

  return lines.join("\n");
}