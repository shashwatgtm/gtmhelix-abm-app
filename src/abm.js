import { z } from "zod";

/* ---------------------------------
   TAXONOMIES
---------------------------------- */

export const STANDARD_VERTICALS = [
  "Technology",
  "Financial Services",
  "Banking",
  "Insurance",
  "Fintech",
  "Healthcare",
  "Life Sciences",
  "Pharma",
  "Cybersecurity",
  "Retail",
  "Consumer Goods",
  "Manufacturing",
  "Logistics",
  "Telecommunications",
  "Media",
  "Energy",
  "Public Sector",
  "Education",
  "HRTech",
  "TravelTech",
  "Other"
];

export const STANDARD_REGIONS = [
  "North America",
  "United States",
  "Canada",
  "LATAM",
  "UKI",
  "DACH",
  "Nordics",
  "Europe",
  "India",
  "APAC",
  "ANZ",
  "MENA",
  "UAE",
  "Global",
  "Other"
];

export const STANDARD_SEGMENTS = [
  "SMB",
  "Mid-Market",
  "Enterprise",
  "Strategic Named Account",
  "Public Sector",
  "Other"
];

export const STANDARD_MOTIONS = [
  "Sales-Led",
  "PLG",
  "Hybrid",
  "Partner-Led",
  "Channel-Led",
  "Other"
];

export const STANDARD_OBJECTIVES = [
  "Net New",
  "Expansion",
  "Cross-Sell",
  "Upsell",
  "Retention",
  "Other"
];

export const STANDARD_STAGES = [
  "Targeting",
  "Target",
  "Awareness",
  "Engaged",
  "Discovery",
  "Evaluation",
  "Meeting",
  "Proposal",
  "Negotiation",
  "Decision",
  "Closed Won",
  "Closed Lost",
  "No Opportunity"
];

export const STANDARD_EMPLOYEE_BANDS = [
  "1-50",
  "51-200",
  "201-1000",
  "1001-5000",
  "5000+",
  "Unknown"
];

const VERTICAL_ALIASES = new Map([
  ["tech", "Technology"],
  ["technology", "Technology"],
  ["software", "Technology"],
  ["saas", "Technology"],
  ["b2b saas", "Technology"],
  ["it", "Technology"],

  ["financial services", "Financial Services"],
  ["financial-services", "Financial Services"],
  ["fsi", "Financial Services"],
  ["bfsi", "Financial Services"],

  ["bank", "Banking"],
  ["banking", "Banking"],

  ["insurance", "Insurance"],
  ["insurtech", "Insurance"],

  ["fintech", "Fintech"],
  ["payments", "Fintech"],

  ["health", "Healthcare"],
  ["healthcare", "Healthcare"],
  ["health care", "Healthcare"],
  ["payer", "Healthcare"],
  ["provider", "Healthcare"],

  ["life sciences", "Life Sciences"],
  ["lifesciences", "Life Sciences"],

  ["pharma", "Pharma"],
  ["pharmaceuticals", "Pharma"],

  ["security", "Cybersecurity"],
  ["cyber", "Cybersecurity"],
  ["cybersecurity", "Cybersecurity"],
  ["cyber security", "Cybersecurity"],

  ["retail", "Retail"],
  ["ecommerce", "Retail"],
  ["e-commerce", "Retail"],

  ["consumer", "Consumer Goods"],
  ["cpg", "Consumer Goods"],
  ["consumer goods", "Consumer Goods"],

  ["manufacturing", "Manufacturing"],
  ["industrial", "Manufacturing"],

  ["logistics", "Logistics"],
  ["supply chain", "Logistics"],
  ["transportation", "Logistics"],

  ["telecom", "Telecommunications"],
  ["telecommunications", "Telecommunications"],

  ["media", "Media"],
  ["advertising", "Media"],

  ["energy", "Energy"],
  ["utilities", "Energy"],

  ["public sector", "Public Sector"],
  ["government", "Public Sector"],
  ["gov", "Public Sector"],

  ["education", "Education"],
  ["edtech", "Education"],

  ["hrtech", "HRTech"],
  ["hr tech", "HRTech"],
  ["people tech", "HRTech"],

  ["travel", "TravelTech"],
  ["traveltech", "TravelTech"],
  ["travel tech", "TravelTech"],

  ["other", "Other"]
]);

const REGION_ALIASES = new Map([
  ["north america", "North America"],
  ["na", "North America"],
  ["usa", "United States"],
  ["us", "United States"],
  ["united states", "United States"],
  ["canada", "Canada"],
  ["latin america", "LATAM"],
  ["latam", "LATAM"],
  ["uk", "UKI"],
  ["ireland", "UKI"],
  ["uki", "UKI"],
  ["dach", "DACH"],
  ["nordics", "Nordics"],
  ["europe", "Europe"],
  ["emea", "Europe"],
  ["india", "India"],
  ["apac", "APAC"],
  ["asia pacific", "APAC"],
  ["anz", "ANZ"],
  ["mena", "MENA"],
  ["uae", "UAE"],
  ["global", "Global"],
  ["other", "Other"]
]);

const SEGMENT_ALIASES = new Map([
  ["smb", "SMB"],
  ["small business", "SMB"],
  ["mid-market", "Mid-Market"],
  ["mid market", "Mid-Market"],
  ["midmarket", "Mid-Market"],
  ["enterprise", "Enterprise"],
  ["strategic named account", "Strategic Named Account"],
  ["named account", "Strategic Named Account"],
  ["strategic", "Strategic Named Account"],
  ["public sector", "Public Sector"],
  ["other", "Other"]
]);

const MOTION_ALIASES = new Map([
  ["sales-led", "Sales-Led"],
  ["sales led", "Sales-Led"],
  ["sales", "Sales-Led"],
  ["plg", "PLG"],
  ["product-led", "PLG"],
  ["product led", "PLG"],
  ["hybrid", "Hybrid"],
  ["partner-led", "Partner-Led"],
  ["partner led", "Partner-Led"],
  ["channel-led", "Channel-Led"],
  ["channel led", "Channel-Led"],
  ["other", "Other"]
]);

const OBJECTIVE_ALIASES = new Map([
  ["net new", "Net New"],
  ["new logo", "Net New"],
  ["expansion", "Expansion"],
  ["cross-sell", "Cross-Sell"],
  ["cross sell", "Cross-Sell"],
  ["upsell", "Upsell"],
  ["retention", "Retention"],
  ["other", "Other"]
]);

const STAGE_ALIASES = new Map([
  ["targeting", "Targeting"],
  ["target", "Target"],
  ["awareness", "Awareness"],
  ["engaged", "Engaged"],
  ["discovery", "Discovery"],
  ["evaluation", "Evaluation"],
  ["meeting", "Meeting"],
  ["proposal", "Proposal"],
  ["negotiation", "Negotiation"],
  ["decision", "Decision"],
  ["closed won", "Closed Won"],
  ["closed lost", "Closed Lost"],
  ["no opportunity", "No Opportunity"]
]);

const ROLE_ALIASES = new Map([
  ["economic buyer", "Economic Buyer"],
  ["budget owner", "Economic Buyer"],
  ["executive sponsor", "Economic Buyer"],
  ["cfo", "Economic Buyer"],
  ["cro", "Economic Buyer"],
  ["coo", "Economic Buyer"],
  ["ceo", "Economic Buyer"],
  ["gm", "Economic Buyer"],
  ["general manager", "Economic Buyer"],

  ["technical buyer", "Technical Buyer"],
  ["cio", "Technical Buyer"],
  ["cto", "Technical Buyer"],
  ["ciso", "Technical Buyer"],
  ["chief digital officer", "Technical Buyer"],
  ["vp it", "Technical Buyer"],
  ["vp engineering", "Technical Buyer"],
  ["head of architecture", "Technical Buyer"],

  ["champion", "Champion"],
  ["user champion", "Champion"],
  ["revops", "Champion"],
  ["vp revops", "Champion"],
  ["director revops", "Champion"],
  ["marketing ops", "Champion"],
  ["sales ops", "Champion"],
  ["program owner", "Champion"],

  ["procurement", "Procurement"],
  ["legal", "Legal"],
  ["security reviewer", "Security Reviewer"],
  ["other", "Other"]
]);

/* ---------------------------------
   NORMALIZATION
---------------------------------- */

function cleanString(v) {
  return String(v ?? "").trim();
}

function lower(v) {
  return cleanString(v).toLowerCase();
}

function uniqueStrings(arr) {
  return [...new Set((arr || []).filter(Boolean))];
}

function normalizeWithAliases(value, aliasMap, allowed, fallback = "Other") {
  const raw = cleanString(value);
  if (!raw) {
    return {
      value: fallback,
      changed: false,
      warning: `Missing value normalized to ${fallback}.`
    };
  }

  if (allowed.includes(raw)) {
    return { value: raw, changed: false, warning: null };
  }

  const aliasHit = aliasMap.get(lower(raw));
  if (aliasHit && allowed.includes(aliasHit)) {
    return {
      value: aliasHit,
      changed: aliasHit !== raw,
      warning: aliasHit !== raw ? `Normalized "${raw}" to "${aliasHit}".` : null
    };
  }

  return {
    value: fallback,
    changed: true,
    warning: `Unrecognized value "${raw}" normalized to "${fallback}".`
  };
}

export function normalizeVertical(value) {
  return normalizeWithAliases(value, VERTICAL_ALIASES, STANDARD_VERTICALS, "Other");
}

export function normalizeRegion(value) {
  return normalizeWithAliases(value, REGION_ALIASES, STANDARD_REGIONS, "Other");
}

export function normalizeSegment(value) {
  return normalizeWithAliases(value, SEGMENT_ALIASES, STANDARD_SEGMENTS, "Other");
}

export function normalizeMotion(value) {
  return normalizeWithAliases(value, MOTION_ALIASES, STANDARD_MOTIONS, "Other");
}

export function normalizeObjective(value) {
  return normalizeWithAliases(value, OBJECTIVE_ALIASES, STANDARD_OBJECTIVES, "Other");
}

export function normalizeStage(value) {
  return normalizeWithAliases(value, STAGE_ALIASES, STANDARD_STAGES, "No Opportunity");
}

export function normalizeRole(value, title = "") {
  const roleAttempt = normalizeWithAliases(
    value,
    ROLE_ALIASES,
    [
      "Economic Buyer",
      "Technical Buyer",
      "Champion",
      "Procurement",
      "Legal",
      "Security Reviewer",
      "Other"
    ],
    "Other"
  );

  if (roleAttempt.value !== "Other" || !cleanString(title)) {
    return roleAttempt;
  }

  const t = lower(title);

  if (/(ceo|cfo|cro|coo|general manager|gm|president|evp|svp)/.test(t)) {
    return {
      value: "Economic Buyer",
      changed: true,
      warning: `Mapped title "${title}" to "Economic Buyer".`
    };
  }

  if (
    /(cio|cto|ciso|chief digital officer|vp it|vp engineering|architecture|platform|infrastructure|data)/.test(
      t
    )
  ) {
    return {
      value: "Technical Buyer",
      changed: true,
      warning: `Mapped title "${title}" to "Technical Buyer".`
    };
  }

  if (
    /(revops|marketing ops|sales ops|operations|program|director|vp digital|head of)/.test(
      t
    )
  ) {
    return {
      value: "Champion",
      changed: true,
      warning: `Mapped title "${title}" to "Champion".`
    };
  }

  return roleAttempt;
}

function normalizeChannels(channels) {
  return uniqueStrings((channels || []).map((c) => cleanString(c)).filter(Boolean));
}

function normalizeTargetRoles(roles) {
  const warnings = [];
  const normalized = uniqueStrings(
    (roles || []).map((r) => {
      const n = normalizeRole(r);
      if (n.warning) warnings.push(n.warning);
      return n.value;
    })
  ).filter((r) => r !== "Other");

  return {
    roles: normalized.length
      ? normalized
      : ["Economic Buyer", "Technical Buyer", "Champion"],
    warnings
  };
}

/* ---------------------------------
   HELPERS
---------------------------------- */

function clamp(n, min, max) {
  const num = Number(n);
  if (!Number.isFinite(num)) return min;
  return Math.max(min, Math.min(max, num));
}

function toOptionalNumber(v) {
  if (v === null || v === undefined || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function toRequiredNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function toBoolean(v) {
  if (typeof v === "boolean") return v;
  const s = lower(v);
  return s === "true" || s === "yes" || s === "1";
}

/* ---------------------------------
   ZOD HELPERS
---------------------------------- */

const nonEmptyString = z
  .string()
  .transform((v) => cleanString(v))
  .pipe(z.string().min(1));

const optionalString = z.any().optional().transform((v) => {
  const s = cleanString(v);
  return s || undefined;
});

const optionalNumber = z.any().optional().transform((v) => toOptionalNumber(v));

const number0to100 = z.any().transform((v) => toRequiredNumber(v)).pipe(z.number().min(0).max(100));
const number0to3650 = z.any().transform((v) => toRequiredNumber(v)).pipe(z.number().min(0).max(3650));

/* ---------------------------------
   SCHEMAS
---------------------------------- */

export const signalSchema = z.object({
  signalType: nonEmptyString,
  strength: number0to100,
  recencyDays: number0to3650
});

export const committeeMemberSchema = z.object({
  name: nonEmptyString,
  title: optionalString,
  role: optionalString,
  relationshipStrength: optionalNumber,
  engaged: z.any().optional().transform((v) => toBoolean(v))
});

export const techSchema = z
  .object({
    crm: optionalString,
    map: optionalString,
    dataWarehouse: optionalString,
    cloud: optionalString,
    securityMaturity: optionalString,
    stackFitScore: optionalNumber
  })
  .default({});

export const accountSchema = z.object({
  accountName: nonEmptyString,
  domain: optionalString,
  vertical: optionalString,
  region: optionalString,
  segment: optionalString,
  gtmMotion: optionalString,
  employeeBand: optionalString,
  openOpportunity: z.any().optional().transform((v) => toBoolean(v)),
  currentPipelineStage: optionalString,
  firstPartyEngagementScore: optionalNumber,
  thirdPartyIntentScore: optionalNumber,
  relationshipScore: optionalNumber,
  signals: z.array(signalSchema).optional().default([]),
  committee: z.array(committeeMemberSchema).optional().default([]),
  tech: techSchema.optional().default({})
});

export const defaultIcpSchema = z.object({
  preferredVerticals: z.array(nonEmptyString).min(1),
  preferredRegions: z.array(nonEmptyString).min(1),
  preferredSegments: z.array(nonEmptyString).min(1),
  preferredMotions: z.array(nonEmptyString).min(1),
  requiredTech: z.array(nonEmptyString).optional().default([]),
  excludedVerticals: z.array(nonEmptyString).optional().default([]),
  excludedRegions: z.array(nonEmptyString).optional().default([]),
  excludedSignals: z.array(nonEmptyString).optional().default([])
});

export const sellerContextSchema = z.object({
  ownerName: nonEmptyString,
  ownerRole: optionalString,
  motionOwner: optionalString,
  channelsAvailable: z.array(nonEmptyString).optional().default([]),
  promotionTriggerDefinition: optionalString
});

export const programSetupSchema = z.object({
  clientName: nonEmptyString,
  objective: nonEmptyString,
  productLine: nonEmptyString,
  defaultTargetRoles: z.array(nonEmptyString).min(1),
  defaultICP: defaultIcpSchema,
  sellerContext: sellerContextSchema,
  enrichmentAllowed: z
    .any()
    .optional()
    .transform((v) => toBoolean(v))
    .default(false)
});

export const abmProjectSchema = z.object({
  clientName: nonEmptyString,
  objective: nonEmptyString,
  productLine: nonEmptyString,
  targetRoles: z.array(nonEmptyString).min(1),
  icp: defaultIcpSchema,
  sellerContext: sellerContextSchema,
  programDefaults: z
    .object({
      enrichmentAllowed: z.boolean().optional().default(false)
    })
    .optional()
    .default({ enrichmentAllowed: false }),
  account: accountSchema
});

export const portfolioSchema = z.object({
  accounts: z.array(z.any()).min(1),
  topN: optionalNumber.default(10)
});

/* ---------------------------------
   ERROR MAPPING
---------------------------------- */

export function validationErrorsToFieldMap(zodError) {
  const out = {};
  for (const issue of zodError.issues ?? []) {
    const key = (issue.path?.join(".") ?? "form").toString();
    out[key] = out[key] ? [...out[key], issue.message] : [issue.message];
  }
  return out;
}

/* ---------------------------------
   PROGRAM NORMALIZATION
---------------------------------- */

function normalizeIcp(icp) {
  const warnings = [];

  const preferredVerticals = uniqueStrings(
    (icp.preferredVerticals || []).map((v) => {
      const n = normalizeVertical(v);
      if (n.warning) warnings.push(`ICP vertical: ${n.warning}`);
      return n.value;
    })
  );

  const preferredRegions = uniqueStrings(
    (icp.preferredRegions || []).map((v) => {
      const n = normalizeRegion(v);
      if (n.warning) warnings.push(`ICP region: ${n.warning}`);
      return n.value;
    })
  );

  const preferredSegments = uniqueStrings(
    (icp.preferredSegments || []).map((v) => {
      const n = normalizeSegment(v);
      if (n.warning) warnings.push(`ICP segment: ${n.warning}`);
      return n.value;
    })
  );

  const preferredMotions = uniqueStrings(
    (icp.preferredMotions || []).map((v) => {
      const n = normalizeMotion(v);
      if (n.warning) warnings.push(`ICP motion: ${n.warning}`);
      return n.value;
    })
  );

  const excludedVerticals = uniqueStrings(
    (icp.excludedVerticals || []).map((v) => normalizeVertical(v).value)
  );
  const excludedRegions = uniqueStrings(
    (icp.excludedRegions || []).map((v) => normalizeRegion(v).value)
  );
  const excludedSignals = uniqueStrings(
    (icp.excludedSignals || []).map((v) => cleanString(v))
  );

  return {
    normalized: {
      preferredVerticals: preferredVerticals.length ? preferredVerticals : ["Other"],
      preferredRegions: preferredRegions.length ? preferredRegions : ["Other"],
      preferredSegments: preferredSegments.length ? preferredSegments : ["Other"],
      preferredMotions: preferredMotions.length ? preferredMotions : ["Other"],
      requiredTech: uniqueStrings((icp.requiredTech || []).map((v) => cleanString(v))),
      excludedVerticals,
      excludedRegions,
      excludedSignals
    },
    warnings
  };
}

export function normalizeProgramSetup(input) {
  const parsed = programSetupSchema.parse(input);

  const objective = normalizeObjective(parsed.objective);
  const targetRoles = normalizeTargetRoles(parsed.defaultTargetRoles);
  const icp = normalizeIcp(parsed.defaultICP);

  const warnings = [
    ...(objective.warning ? [`Objective: ${objective.warning}`] : []),
    ...targetRoles.warnings,
    ...icp.warnings
  ];

  return {
    clientName: parsed.clientName,
    objective: objective.value,
    productLine: parsed.productLine,
    defaultTargetRoles: targetRoles.roles,
    defaultICP: icp.normalized,
    sellerContext: {
      ownerName: parsed.sellerContext.ownerName,
      ownerRole: parsed.sellerContext.ownerRole || null,
      motionOwner: parsed.sellerContext.motionOwner || null,
      channelsAvailable: normalizeChannels(parsed.sellerContext.channelsAvailable),
      promotionTriggerDefinition:
        parsed.sellerContext.promotionTriggerDefinition ||
        "Promote when fit, demand, and committee evidence justify higher investment."
    },
    enrichmentAllowed: Boolean(parsed.enrichmentAllowed),
    normalizationWarnings: uniqueStrings(warnings)
  };
}

/* ---------------------------------
   ACCOUNT NORMALIZATION
---------------------------------- */

export function normalizeAccount(account) {
  const warnings = [];

  const vertical = normalizeVertical(account.vertical);
  const region = normalizeRegion(account.region);
  const segment = normalizeSegment(account.segment);
  const gtmMotion = normalizeMotion(account.gtmMotion);
  const stage = normalizeStage(account.currentPipelineStage);

  if (vertical.warning) warnings.push(`Account vertical: ${vertical.warning}`);
  if (region.warning) warnings.push(`Account region: ${region.warning}`);
  if (segment.warning) warnings.push(`Account segment: ${segment.warning}`);
  if (gtmMotion.warning) warnings.push(`Account motion: ${gtmMotion.warning}`);
  if (stage.warning) warnings.push(`Account stage: ${stage.warning}`);

  const employeeBandRaw = cleanString(account.employeeBand || "Unknown");
  const employeeBand = STANDARD_EMPLOYEE_BANDS.includes(employeeBandRaw)
    ? employeeBandRaw
    : "Unknown";
  if (employeeBandRaw && employeeBandRaw !== employeeBand) {
    warnings.push(`Employee band "${employeeBandRaw}" normalized to "Unknown".`);
  }

  const committee = (account.committee || []).map((m) => {
    const role = normalizeRole(m.role, m.title);
    if (role.warning) warnings.push(`Committee role: ${role.warning}`);
    return {
      name: m.name,
      title: m.title || null,
      role: role.value,
      relationshipStrength: clamp(m.relationshipStrength ?? 0, 0, 100),
      engaged: Boolean(m.engaged)
    };
  });

  const signals = (account.signals || [])
    .map((s) => ({
      signalType: cleanString(s.signalType),
      strength: clamp(s.strength ?? 0, 0, 100),
      recencyDays: clamp(s.recencyDays ?? 3650, 0, 3650)
    }))
    .filter((s) => s.signalType);

  return {
    normalized: {
      accountName: account.accountName,
      domain: account.domain || null,
      vertical: vertical.value,
      region: region.value,
      segment: segment.value,
      gtmMotion: gtmMotion.value,
      employeeBand,
      openOpportunity: Boolean(account.openOpportunity),
      currentPipelineStage: stage.value,
      firstPartyEngagementScore: clamp(account.firstPartyEngagementScore ?? 0, 0, 100),
      thirdPartyIntentScore: clamp(account.thirdPartyIntentScore ?? 0, 0, 100),
      relationshipScore: clamp(account.relationshipScore ?? 0, 0, 100),
      signals,
      committee,
      tech: {
        crm: account.tech?.crm || null,
        map: account.tech?.map || null,
        dataWarehouse: account.tech?.dataWarehouse || null,
        cloud: account.tech?.cloud || null,
        securityMaturity: account.tech?.securityMaturity || null,
        stackFitScore: clamp(account.tech?.stackFitScore ?? 0, 0, 100)
      }
    },
    warnings: uniqueStrings(warnings)
  };
}

/* ---------------------------------
   SCORING
---------------------------------- */

function hasRecentBottomFunnelSignal(signals) {
  return (signals || []).some((s) => {
    const t = lower(s.signalType);
    return (
      s.recencyDays <= 14 &&
      (t.includes("demo") ||
        t.includes("pricing") ||
        t.includes("reply") ||
        t.includes("meeting") ||
        t.includes("trial"))
    );
  });
}

function stageWeight(stage) {
  switch (stage) {
    case "Decision":
      return 95;
    case "Negotiation":
      return 90;
    case "Proposal":
      return 82;
    case "Evaluation":
      return 75;
    case "Meeting":
      return 68;
    case "Discovery":
      return 62;
    case "Engaged":
      return 50;
    case "Awareness":
      return 38;
    case "Target":
    case "Targeting":
      return 25;
    case "Closed Won":
      return 100;
    case "Closed Lost":
      return 5;
    case "No Opportunity":
    default:
      return 12;
  }
}

function confidenceFromMissingness(
  missingDataWarnings,
  normalizationWarnings,
  exploriumDiagnostics
) {
  let score = 100;
  score -= (missingDataWarnings?.length || 0) * 7;
  score -= (normalizationWarnings?.length || 0) * 3;

  if (exploriumDiagnostics?.attempted && !exploriumDiagnostics?.matched) {
    score -= 4;
  }

  return clamp(score, 30, 99);
}

function fitScore(project, normalizedAccount) {
  const reasons = [];
  const risks = [];
  let score = 0;

  if (project.icp.preferredVerticals.includes(normalizedAccount.vertical)) {
    score += 18;
    reasons.push(`Vertical fit: ${normalizedAccount.vertical}`);
  } else if (normalizedAccount.vertical === "Other") {
    score += 6;
    risks.push("Vertical was unknown or normalized to Other.");
  } else {
    risks.push(`Vertical misfit: ${normalizedAccount.vertical}`);
  }

  if (project.icp.preferredRegions.includes(normalizedAccount.region)) {
    score += 14;
    reasons.push(`Regional fit: ${normalizedAccount.region}`);
  } else if (normalizedAccount.region === "Other") {
    score += 5;
    risks.push("Region was unknown or normalized to Other.");
  } else {
    risks.push(`Regional misfit: ${normalizedAccount.region}`);
  }

  if (project.icp.preferredSegments.includes(normalizedAccount.segment)) {
    score += 14;
    reasons.push(`Segment fit: ${normalizedAccount.segment}`);
  } else if (normalizedAccount.segment === "Other") {
    score += 4;
    risks.push("Segment was unknown or normalized to Other.");
  } else {
    risks.push(`Segment misfit: ${normalizedAccount.segment}`);
  }

  if (project.icp.preferredMotions.includes(normalizedAccount.gtmMotion)) {
    score += 10;
    reasons.push(`GTM motion fit: ${normalizedAccount.gtmMotion}`);
  } else if (normalizedAccount.gtmMotion === "Other") {
    score += 3;
    risks.push("Motion was unknown or normalized to Other.");
  } else {
    risks.push(`Motion misfit: ${normalizedAccount.gtmMotion}`);
  }

  const techFit = clamp(((normalizedAccount.tech?.stackFitScore ?? 0) / 100) * 12, 0, 12);
  score += techFit;
  if (techFit > 0) {
    reasons.push(`Technographic fit contributes ${Math.round(techFit)} points`);
  }

  if (normalizedAccount.employeeBand === "Unknown") {
    risks.push("Employee band missing. Firmographic fit is partially inferred.");
  } else {
    reasons.push(`Employee band available: ${normalizedAccount.employeeBand}`);
  }

  return {
    score: clamp(Math.round(score), 0, 100),
    reasons,
    risks
  };
}

function intentScore(normalizedAccount) {
  const reasons = [];
  const topSignals = [];
  const signalPoints = (normalizedAccount.signals || []).reduce((sum, s) => {
    const freshness = Math.max(0.25, 1 - s.recencyDays / 90);
    const points = s.strength * freshness;
    topSignals.push(`${s.signalType} (${s.strength}, ${s.recencyDays}d)`);
    return sum + points;
  }, 0);

  const blended =
    normalizedAccount.firstPartyEngagementScore * 0.35 +
    normalizedAccount.thirdPartyIntentScore * 0.45 +
    Math.min(signalPoints, 100) * 0.2;

  if (normalizedAccount.firstPartyEngagementScore > 0) {
    reasons.push(`First-party engagement: ${normalizedAccount.firstPartyEngagementScore}`);
  }
  if (normalizedAccount.thirdPartyIntentScore > 0) {
    reasons.push(`Third-party intent: ${normalizedAccount.thirdPartyIntentScore}`);
  }
  if (topSignals.length) {
    reasons.push("Recent signals available");
  }

  return {
    score: clamp(Math.round(blended), 0, 100),
    reasons,
    topSignals: topSignals.slice(0, 5)
  };
}

function committeeCoverage(project, normalizedAccount) {
  const targetRoles = project.targetRoles || [];
  const covered = new Set();
  const warnings = [];
  const members = normalizedAccount.committee || [];

  for (const member of members) {
    if (targetRoles.includes(member.role)) {
      covered.add(member.role);
    }
  }

  const roleCoveragePoints = targetRoles.length ? (covered.size / targetRoles.length) * 70 : 0;

  const relationshipBonus = members.length
    ? Math.min(
        30,
        members.reduce(
          (sum, m) => sum + (m.engaged ? 10 : 0) + ((m.relationshipStrength || 0) / 10),
          0
        )
      )
    : 0;

  if (!members.length) {
    warnings.push(
      "No buying committee data provided. Committee coverage is inferred from target roles and marked low-confidence."
    );
  }

  const missingRoles = targetRoles.filter((r) => !covered.has(r));

  return {
    score: clamp(Math.round(roleCoveragePoints + relationshipBonus), 0, 100),
    coveredRoles: [...covered],
    missingRoles,
    warnings
  };
}

function relationshipComponent(normalizedAccount) {
  const averageCommitteeStrength =
    (normalizedAccount.committee || []).reduce(
      (sum, m) => sum + (m.relationshipStrength || 0),
      0
    ) / Math.max(1, normalizedAccount.committee.length);

  const score = clamp(
    Math.round(
      normalizedAccount.relationshipScore * 0.65 + averageCommitteeStrength * 0.35
    ),
    0,
    100
  );

  return { score };
}

function inferBuyingStage(normalizedAccount, intent, committee) {
  const stageBase = stageWeight(normalizedAccount.currentPipelineStage);
  const signalBonus = hasRecentBottomFunnelSignal(normalizedAccount.signals) ? 12 : 0;
  const committeeBonus =
    committee.coveredRoles.length >= 2 ? 8 : committee.coveredRoles.length >= 1 ? 4 : 0;
  const oppBonus = normalizedAccount.openOpportunity ? 10 : 0;
  const composite = clamp(
    Math.round(stageBase * 0.5 + intent.score * 0.3 + signalBonus + committeeBonus + oppBonus),
    0,
    100
  );

  let buyingStage = "Target";
  let stageConfidence = "low";

  if (composite >= 80) {
    buyingStage = "Decision";
    stageConfidence = "high";
  } else if (composite >= 65) {
    buyingStage = "Consideration";
    stageConfidence = "high";
  } else if (composite >= 45) {
    buyingStage = "Awareness";
    stageConfidence = "medium";
  }

  return { buyingStage, stageConfidence, composite };
}

function computePenalties(project, normalizedAccount, fit, intent, committee) {
  const penalties = [];

  if (fit.score < 40) penalties.push("Fit score below hard promotion floor.");
  if (
    !normalizedAccount.openOpportunity &&
    normalizedAccount.currentPipelineStage === "No Opportunity"
  ) {
    penalties.push("No active opportunity.");
  }
  if (intent.score < 30) penalties.push("Weak or absent demand signal.");
  if (!committee.coveredRoles.includes("Economic Buyer")) {
    penalties.push("Economic buyer not mapped.");
  }
  if (!committee.coveredRoles.includes("Technical Buyer")) {
    penalties.push("Technical buyer not mapped.");
  }
  if (!normalizedAccount.signals.length && normalizedAccount.thirdPartyIntentScore === 0) {
    penalties.push("No intent instrumentation.");
  }
  if (project.icp.excludedVerticals?.includes(normalizedAccount.vertical)) {
    penalties.push(`Vertical is explicitly excluded: ${normalizedAccount.vertical}`);
  }
  if (project.icp.excludedRegions?.includes(normalizedAccount.region)) {
    penalties.push(`Region is explicitly excluded: ${normalizedAccount.region}`);
  }

  return penalties;
}

function tierAndAction(project, normalizedAccount, fit, intent, committee, relationship, penalties) {
  const weighted =
    fit.score * 0.4 +
    intent.score * 0.3 +
    committee.score * 0.2 +
    relationship.score * 0.1;

  const hardNoPromote = fit.score < 40;

  const tier2Eligible =
    fit.score >= 55 &&
    (
      intent.score >= 55 ||
      (normalizedAccount.openOpportunity &&
        stageWeight(normalizedAccount.currentPipelineStage) >= stageWeight("Discovery")) ||
      hasRecentBottomFunnelSignal(normalizedAccount.signals)
    ) &&
    (
      committee.score >= 35 ||
      committee.coveredRoles.length >= 1 ||
      (normalizedAccount.committee || []).some((m) =>
        /(vp|chief|cio|cto|cfo|cro|coo|ceo|evp|svp)/i.test(m.title || "")
      )
    );

  let totalScore = clamp(Math.round(weighted - penalties.length * 3), 0, 100);
  let tier = "Tier 3";
  let ownerAction = "Keep in monitored nurture";
  let minimumNextStep = "Add to monitored nurture";

  if (!hardNoPromote && tier2Eligible) {
    tier = "Tier 2";
    ownerAction = "Run tailored 1:few motion";
    minimumNextStep = "Run tailored outbound + paid sequence";
    totalScore = Math.max(totalScore, 55);
  }

  if (
    !hardNoPromote &&
    fit.score >= 70 &&
    intent.score >= 75 &&
    committee.coveredRoles.length >= 2 &&
    normalizedAccount.openOpportunity
  ) {
    tier = "Tier 1";
    ownerAction = "Run bespoke 1:1 ABM motion";
    minimumNextStep = "Coordinate exec outreach + multithreaded account plan";
    totalScore = Math.max(totalScore, 75);
  }

  return {
    totalScore: clamp(totalScore, 0, 100),
    tier,
    ownerAction,
    minimumNextStep
  };
}

function messageHypothesis(project, normalizedAccount, fit, intent) {
  const vertical = normalizedAccount.vertical || "target vertical";
  const product = project.productLine || "your solution";

  if (fit.score >= 60 && intent.score >= 55) {
    return `Lead with a ${vertical}-specific commercial case for ${product}, tied to measurable revenue or operating lift and backed by role-relevant proof.`;
  }

  if (fit.score >= 55) {
    return `Lead with strategic relevance for ${vertical}, but keep the ask light until stronger demand or stakeholder access appears.`;
  }

  return `Keep messaging broad and educational; do not overpersonalize until fit or demand improves.`;
}

function recommendedPlays(project, normalizedAccount, fit, intent, committee, tier) {
  const plays = [];

  if (tier === "Tier 1") {
    plays.push("Run 1:1 account plan with multithreaded outreach.");
    plays.push("Build executive air cover and stakeholder map.");
    plays.push("Use ROI, migration, and proof assets tailored to the account.");
  } else if (tier === "Tier 2") {
    plays.push("Run 1:few vertical/region play with tailored proof.");
    plays.push("Coordinate SDR + paid + content sequence.");
    plays.push(`Map missing roles: ${committee.missingRoles.join(", ") || "None"}.`);
  } else {
    plays.push("Run 1:many nurture with lightweight monitoring.");
    plays.push("Promote only if fit or signals improve.");
    plays.push("Use awareness and insight-led air cover.");
    if (committee.missingRoles.length) {
      plays.push(`Map missing roles: ${committee.missingRoles.join(", ")}.`);
    }
  }

  if (hasRecentBottomFunnelSignal(normalizedAccount.signals)) {
    plays.push("Follow up quickly on recent bottom-funnel behavior.");
  }

  return uniqueStrings(plays);
}

function executionPlan(project, normalizedAccount, tier) {
  if (tier === "Tier 1") {
    return [
      "Build 1:1 account brief and executive hypothesis",
      "Coordinate AE, SDR, marketing, and leadership outreach",
      "Run weekly deal and stakeholder review"
    ];
  }

  if (tier === "Tier 2") {
    return [
      "Run tailored outbound + paid sequence",
      "Use vertical and regional proof",
      "Run weekly account review"
    ];
  }

  return [
    "Add to monitored nurture",
    "Promote on new signal or stakeholder access",
    "Run fortnightly signal review"
  ];
}

/* ---------------------------------
   EXPLORIUM DIAGNOSTICS SHAPING
---------------------------------- */

export function normalizeExploriumDiagnostics(raw) {
  const attempted = Boolean(raw?.attempted);
  const matched = Boolean(raw?.matched);
  const businessId = raw?.businessId || raw?.business_id || null;

  const reason =
    raw?.reason ||
    raw?.warning ||
    raw?.error ||
    (attempted && !matched
      ? "Explorium enrichment failed; continuing without enrichment."
      : null);

  const fieldsAdded = Array.isArray(raw?.fieldsAdded) ? raw.fieldsAdded : [];
  const sourceData = raw?.sourceData || null;

  return {
    attempted,
    matched,
    businessId,
    reason,
    fieldsAdded,
    sourceData
  };
}

/* ---------------------------------
   PROJECT INTELLIGENCE
---------------------------------- */

export function buildAbmProjectIntel(projectInput, options = {}) {
  const parsed = abmProjectSchema.parse(projectInput);

  const normalizedSetup = normalizeProgramSetup({
    clientName: parsed.clientName,
    objective: parsed.objective,
    productLine: parsed.productLine,
    defaultTargetRoles: parsed.targetRoles,
    defaultICP: parsed.icp,
    sellerContext: parsed.sellerContext,
    enrichmentAllowed: parsed.programDefaults?.enrichmentAllowed ?? false
  });

  const normalizedAccountResult = normalizeAccount(parsed.account);
  const normalizedAccount = normalizedAccountResult.normalized;

  const project = {
    clientName: normalizedSetup.clientName,
    objective: normalizedSetup.objective,
    productLine: normalizedSetup.productLine,
    targetRoles: normalizedSetup.defaultTargetRoles,
    icp: normalizedSetup.defaultICP,
    sellerContext: normalizedSetup.sellerContext,
    enrichmentAllowed: normalizedSetup.enrichmentAllowed
  };

  const fit = fitScore(project, normalizedAccount);
  const intent = intentScore(normalizedAccount);
  const committee = committeeCoverage(project, normalizedAccount);
  const relationship = relationshipComponent(normalizedAccount);
  const stage = inferBuyingStage(normalizedAccount, intent, committee);
  const exploriumDiagnostics = normalizeExploriumDiagnostics(options.exploriumDiagnostics);
  const penalties = computePenalties(project, normalizedAccount, fit, intent, committee);
  const tiering = tierAndAction(
    project,
    normalizedAccount,
    fit,
    intent,
    committee,
    relationship,
    penalties
  );

  const missingDataWarnings = [];
  if (normalizedAccount.employeeBand === "Unknown") {
    missingDataWarnings.push("Employee band missing. Firmographic fit is partially inferred.");
  }
  if (!normalizedAccount.tech?.securityMaturity) {
    missingDataWarnings.push("Security maturity unavailable; conservative baseline applied.");
  }
  if (!normalizedAccount.committee.length) {
    missingDataWarnings.push(
      "No buying committee data provided. Committee coverage is inferred from target roles and marked low-confidence."
    );
  }

  if (
    exploriumDiagnostics.attempted &&
    !exploriumDiagnostics.matched &&
    exploriumDiagnostics.reason
  ) {
    missingDataWarnings.push(exploriumDiagnostics.reason);
  }

  const confidenceValue = confidenceFromMissingness(
    uniqueStrings([...missingDataWarnings, ...committee.warnings]),
    uniqueStrings([
      ...normalizedSetup.normalizationWarnings,
      ...normalizedAccountResult.warnings
    ]),
    exploriumDiagnostics
  );

  const plays = recommendedPlays(project, normalizedAccount, fit, intent, committee, tiering.tier);
  const plan = executionPlan(project, normalizedAccount, tiering.tier);

  return {
    clientName: project.clientName,
    objective: project.objective,
    productLine: project.productLine,
    accountName: normalizedAccount.accountName,
    accountDomain: normalizedAccount.domain,
    normalizedFields: {
      vertical: normalizedAccount.vertical,
      region: normalizedAccount.region,
      segment: normalizedAccount.segment,
      gtmMotion: normalizedAccount.gtmMotion,
      currentPipelineStage: normalizedAccount.currentPipelineStage
    },
    normalizationWarnings: uniqueStrings([
      ...normalizedSetup.normalizationWarnings,
      ...normalizedAccountResult.warnings
    ]),
    scores: {
      fit: fit.score,
      intent: intent.score,
      committeeCoverage: committee.score,
      relationship: relationship.score
    },
    totalScore: tiering.totalScore,
    tier: tiering.tier,
    buyingStage: stage.buyingStage,
    stageConfidence: stage.stageConfidence,
    overallConfidence: confidenceValue,
    confidence: {
      overallConfidence: confidenceValue
    },
    ownerAction: tiering.ownerAction,
    minimumNextStep: tiering.minimumNextStep,
    promotionCriteria:
      project.sellerContext.promotionTriggerDefinition ||
      "Promote when fit, demand, and committee evidence justify higher investment.",
    doNotDo:
      tiering.tier === "Tier 3"
        ? "Do not overinvest in bespoke content or heavy paid spend."
        : "Do not skip committee mapping.",
    messageHypothesis: messageHypothesis(project, normalizedAccount, fit, intent),
    recommendedPlays: plays,
    plays,
    executionPlan: plan,
    watchouts: uniqueStrings([...penalties, ...committee.warnings]),
    missingRoles: committee.missingRoles,
    missingDataWarnings: uniqueStrings(missingDataWarnings),
    fitReasons: fit.reasons,
    fitRisks: fit.risks,
    topSignals: intent.topSignals,
    coveredRoles: committee.coveredRoles,
    penalties,
    explainability: {
      missingRoles: committee.missingRoles,
      missingDataWarnings: uniqueStrings(missingDataWarnings),
      fitReasons: fit.reasons,
      fitRisks: fit.risks,
      topSignals: intent.topSignals,
      coveredRoles: committee.coveredRoles,
      penalties
    },
    explorium: {
      attempted: exploriumDiagnostics.attempted,
      matched: exploriumDiagnostics.matched,
      businessId: exploriumDiagnostics.businessId,
      reason: exploriumDiagnostics.reason,
      fieldsAdded: exploriumDiagnostics.fieldsAdded
    },
    assumptionsUsed: {
      clientName: project.clientName,
      objective: project.objective,
      productLine: project.productLine,
      targetRoles: project.targetRoles,
      icp: project.icp,
      sellerContext: project.sellerContext
    },
    sourceProvenance: {
      userProvided: [
        "clientName",
        "objective",
        "productLine",
        "targetRoles",
        "ICP preferences",
        "sellerContext",
        "account row fields",
        "signals",
        "committee members"
      ],
      enrichedFacts: exploriumDiagnostics.matched
        ? exploriumDiagnostics.fieldsAdded?.length
          ? exploriumDiagnostics.fieldsAdded
          : ["Explorium matched account enrichment"]
        : [],
      derivedByEngine: [
        "normalized fields",
        "fit score",
        "intent score",
        "committee coverage score",
        "relationship score",
        "total score",
        "tier",
        "buying stage",
        "confidence",
        "plays",
        "execution plan",
        "watchouts"
      ]
    }
  };
}

/* ---------------------------------
   PORTFOLIO
---------------------------------- */

function toProjectShape(accountProject) {
  if (accountProject?.account && accountProject?.icp && accountProject?.sellerContext) {
    return accountProject;
  }

  return {
    clientName: accountProject?.clientName || "Unknown Client",
    objective: accountProject?.objective || "Other",
    productLine: accountProject?.productLine || "Unknown Product",
    targetRoles: Array.isArray(accountProject?.targetRoles) && accountProject.targetRoles.length
      ? accountProject.targetRoles
      : ["Economic Buyer", "Technical Buyer", "Champion"],
    icp: accountProject?.icp || {
      preferredVerticals: ["Other"],
      preferredRegions: ["Other"],
      preferredSegments: ["Other"],
      preferredMotions: ["Other"],
      requiredTech: [],
      excludedVerticals: [],
      excludedRegions: [],
      excludedSignals: []
    },
    sellerContext: accountProject?.sellerContext || {
      ownerName: "Unknown Owner",
      ownerRole: null,
      motionOwner: null,
      channelsAvailable: [],
      promotionTriggerDefinition:
        "Promote when fit, demand, and committee evidence justify higher investment."
    },
    programDefaults: accountProject?.programDefaults || {
      enrichmentAllowed: false
    },
    account: accountProject?.account || {
      accountName: accountProject?.accountName || "Unknown Account",
      vertical: accountProject?.vertical || "Other",
      region: accountProject?.region || "Other",
      segment: accountProject?.segment || "Other",
      gtmMotion: "Other",
      currentPipelineStage: "No Opportunity",
      signals: [],
      committee: [],
      tech: {}
    }
  };
}

export function prioritizePortfolio(portfolioInput) {
  const parsed = portfolioSchema.parse(portfolioInput);

  const scored = parsed.accounts.map((accountProject) => {
    const intel = buildAbmProjectIntel(toProjectShape(accountProject));
    return {
      accountName: intel.accountName,
      accountDomain: intel.accountDomain,
      totalScore: intel.totalScore,
      tier: intel.tier,
      buyingStage: intel.buyingStage,
      overallConfidence: intel.overallConfidence,
      ownerAction: intel.ownerAction,
      minimumNextStep: intel.minimumNextStep,
      missingRoles: intel.missingRoles,
      fitReasons: intel.fitReasons,
      penalties: intel.penalties,
      fullIntelligence: intel
    };
  });

  scored.sort((a, b) => b.totalScore - a.totalScore);

  const counts = scored.reduce(
    (acc, row) => {
      acc[row.tier] = (acc[row.tier] || 0) + 1;
      return acc;
    },
    { "Tier 1": 0, "Tier 2": 0, "Tier 3": 0 }
  );

  return {
    totalAccounts: scored.length,
    summary: counts,
    topAccounts: scored.slice(0, Math.max(1, Number(parsed.topN || 10))),
    allAccounts: scored
  };
}

/* ---------------------------------
   CSV BATCH
---------------------------------- */

function splitCsvLine(line) {
  const out = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"' && inQuotes && next === '"') {
      current += '"';
      i += 1;
      continue;
    }

    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(current);
      current = "";
      continue;
    }

    current += ch;
  }

  out.push(current);
  return out;
}

function parseCsv(text) {
  const lines = String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .filter((line) => line.trim().length > 0);

  if (!lines.length) {
    return { headers: [], rows: [] };
  }

  const headers = splitCsvLine(lines[0]).map((h) => cleanString(h));
  const rows = lines.slice(1).map((line) => {
    const cells = splitCsvLine(line);
    const row = {};
    headers.forEach((header, idx) => {
      row[header] = cleanString(cells[idx] ?? "");
    });
    return row;
  });

  return { headers, rows };
}

function parseSignalsCell(value) {
  if (!cleanString(value)) return [];
  return value
    .split("|")
    .map((chunk) => {
      const [signalType, strength, recencyDays] = chunk.split(":");
      return {
        signalType: cleanString(signalType),
        strength: Number(strength || 0),
        recencyDays: Number(recencyDays || 3650)
      };
    })
    .filter((s) => s.signalType);
}

function parseCommitteeCell(value) {
  if (!cleanString(value)) return [];
  return value
    .split("|")
    .map((chunk) => {
      const [name, title, role, relationshipStrength, engaged] = chunk.split(";");
      return {
        name: cleanString(name),
        title: cleanString(title),
        role: cleanString(role),
        relationshipStrength: Number(relationshipStrength || 0),
        engaged: lower(engaged) === "true"
      };
    })
    .filter((m) => m.name);
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export function scoreAbmCsvBatch({ csvText, programSetup, enrichRow }) {
  const setup = normalizeProgramSetup(programSetup);
  const parsedCsv = parseCsv(csvText);

  const scoredRows = [];
  const rowErrors = [];
  const normalizationWarnings = [];
  const enrichmentSummary = [];

  for (let index = 0; index < parsedCsv.rows.length; index += 1) {
    const row = parsedCsv.rows[index];
    try {
      const account = {
        accountName: row.account_name || row.accountName,
        domain: row.account_domain || row.accountDomain || undefined,
        vertical: row.vertical || undefined,
        region: row.region || undefined,
        segment: row.segment || undefined,
        gtmMotion: row.gtm_motion || row.gtmMotion || undefined,
        employeeBand: row.employee_band || row.employeeBand || undefined,
        openOpportunity: row.open_opportunity || row.openOpportunity || false,
        currentPipelineStage:
          row.current_pipeline_stage || row.currentPipelineStage || "No Opportunity",
        firstPartyEngagementScore:
          row.first_party_engagement_score || row.firstPartyEngagementScore || 0,
        thirdPartyIntentScore:
          row.third_party_intent_score || row.thirdPartyIntentScore || 0,
        relationshipScore: row.relationship_score || row.relationshipScore || 0,
        signals: parseSignalsCell(row.signals),
        committee: parseCommitteeCell(row.committee_members || row.committee)
      };

      const baseProject = {
        clientName: setup.clientName,
        objective: setup.objective,
        productLine: setup.productLine,
        targetRoles: setup.defaultTargetRoles,
        icp: setup.defaultICP,
        sellerContext: setup.sellerContext,
        programDefaults: { enrichmentAllowed: setup.enrichmentAllowed },
        account
      };

      const projectParsed = abmProjectSchema.safeParse(baseProject);
      if (!projectParsed.success) {
        rowErrors.push({
          rowNumber: index + 2,
          accountName: account.accountName || null,
          errors: validationErrorsToFieldMap(projectParsed.error)
        });
        continue;
      }

      let exploriumDiagnostics = {
        attempted: false,
        matched: false,
        businessId: null,
        reason: null,
        fieldsAdded: []
      };

      if (setup.enrichmentAllowed && typeof enrichRow === "function") {
        try {
          const enrichment = enrichRow(projectParsed.data);
          exploriumDiagnostics = normalizeExploriumDiagnostics(enrichment);
        } catch (e) {
          exploriumDiagnostics = normalizeExploriumDiagnostics({
            attempted: true,
            matched: false,
            reason: e?.message || "Explorium enrichment threw an unexpected error."
          });
        }
      }

      const intel = buildAbmProjectIntel(projectParsed.data, { exploriumDiagnostics });

      normalizationWarnings.push(
        ...intel.normalizationWarnings.map((w) => ({
          rowNumber: index + 2,
          accountName: intel.accountName,
          warning: w
        }))
      );

      enrichmentSummary.push({
        rowNumber: index + 2,
        accountName: intel.accountName,
        attempted: intel.explorium.attempted,
        matched: intel.explorium.matched,
        businessId: intel.explorium.businessId,
        reason: intel.explorium.reason,
        fieldsAdded: intel.explorium.fieldsAdded
      });

      scoredRows.push(intel);
    } catch (e) {
      rowErrors.push({
        rowNumber: index + 2,
        accountName: row.account_name || row.accountName || null,
        errors: { form: [e?.message || "Unexpected batch scoring error."] }
      });
    }
  }

  const summary = {
    totalRows: parsedCsv.rows.length,
    scoredRows: scoredRows.length,
    errorRows: rowErrors.length,
    tier1: scoredRows.filter((r) => r.tier === "Tier 1").length,
    tier2: scoredRows.filter((r) => r.tier === "Tier 2").length,
    tier3: scoredRows.filter((r) => r.tier === "Tier 3").length
  };

  scoredRows.sort((a, b) => b.totalScore - a.totalScore);

  const outputHeaders = [
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
    "normalizationWarnings",
    "exploriumAttempted",
    "exploriumMatched",
    "exploriumBusinessId",
    "exploriumReason",
    "exploriumFieldsAdded",
    "fitReasons",
    "fitRisks",
    "topSignals",
    "coveredRoles",
    "penalties"
  ];

  const outputRows = scoredRows.map((r) => ({
    clientName: r.clientName,
    accountName: r.accountName,
    accountDomain: r.accountDomain || "",
    vertical: r.normalizedFields.vertical,
    region: r.normalizedFields.region,
    segment: r.normalizedFields.segment,
    objective: r.objective,
    productLine: r.productLine,
    totalScore: r.totalScore,
    tier: r.tier,
    buyingStage: r.buyingStage,
    stageConfidence: r.stageConfidence,
    fitScore: r.scores.fit,
    intentScore: r.scores.intent,
    committeeCoverageScore: r.scores.committeeCoverage,
    relationshipScore: r.scores.relationship,
    overallConfidence: r.overallConfidence,
    ownerAction: r.ownerAction,
    minimumNextStep: r.minimumNextStep,
    promotionCriteria: r.promotionCriteria,
    doNotDo: r.doNotDo,
    messageHypothesis: r.messageHypothesis,
    recommendedPlays: r.recommendedPlays.join(" | "),
    executionPlan: r.executionPlan.join(" | "),
    watchouts: r.watchouts.join(" | "),
    missingRoles: r.missingRoles.join(" | "),
    missingDataWarnings: r.missingDataWarnings.join(" | "),
    normalizationWarnings: r.normalizationWarnings.join(" | "),
    exploriumAttempted: r.explorium.attempted,
    exploriumMatched: r.explorium.matched,
    exploriumBusinessId: r.explorium.businessId || "",
    exploriumReason: r.explorium.reason || "",
    exploriumFieldsAdded: (r.explorium.fieldsAdded || []).join(" | "),
    fitReasons: r.fitReasons.join(" | "),
    fitRisks: r.fitRisks.join(" | "),
    topSignals: r.topSignals.join(" | "),
    coveredRoles: r.coveredRoles.join(" | "),
    penalties: r.penalties.join(" | ")
  }));

  const outputCsv = [
    outputHeaders.join(","),
    ...outputRows.map((row) => outputHeaders.map((h) => csvEscape(row[h])).join(","))
  ].join("\n");

  return {
    programSetup: setup,
    summary,
    topAccounts: scoredRows.slice(0, 10).map((r) => ({
      accountName: r.accountName,
      totalScore: r.totalScore,
      tier: r.tier,
      buyingStage: r.buyingStage,
      overallConfidence: r.overallConfidence,
      ownerAction: r.ownerAction,
      minimumNextStep: r.minimumNextStep,
      missingRoles: r.missingRoles
    })),
    scoredRows,
    rowErrors,
    normalizationWarnings,
    enrichmentSummary,
    outputCsv
  };
}