import { z } from "zod";

/* -----------------------------
   TAXONOMY
------------------------------ */

export const VERTICALS = [
  "Cybersecurity",
  "Fintech",
  "Healthcare",
  "Pharma",
  "Logistics",
  "TravelTech",
  "HRtech",
  "Retail",
  "Manufacturing",
  "Telecom",
  "Media",
  "Public Sector",
  "Other"
];

export const REGIONS = [
  "North America",
  "UKI",
  "DACH",
  "Nordics",
  "Southern Europe",
  "India",
  "SEA",
  "ANZ",
  "Middle East",
  "LATAM",
  "Global"
];

export const ACCOUNT_SEGMENTS = [
  "Enterprise",
  "Mid-Market",
  "SMB",
  "Strategic Named Account",
  "Channel / Partner"
];

export const GTM_MOTIONS = [
  "Enterprise Sales",
  "PLG",
  "Hybrid",
  "Channel-Led",
  "Sales-Led"
];

export const BUYING_ROLES = [
  "Economic Buyer",
  "Technical Buyer",
  "Champion",
  "End User Lead",
  "Security / Compliance",
  "Procurement",
  "Executive Sponsor"
];

/* -----------------------------
   HELPERS
------------------------------ */

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function avg(nums) {
  if (!nums.length) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

function unique(arr) {
  return [...new Set(arr)];
}

const employeeBandOrder = ["1-50", "51-200", "201-1000", "1001-5000", "5000+"];
const maturityOrder = ["Low", "Moderate", "High", "Very High"];

function compareOrdered(actual, minimum, order) {
  if (!actual || !minimum) return false;
  return order.indexOf(actual) >= order.indexOf(minimum);
}

/* -----------------------------
   PROGRAM SETUP SCHEMA
------------------------------ */

export const programSetupSchema = z.object({
  clientName: z.string().min(2),
  objective: z.enum(["Net New", "Expansion", "Cross-Sell", "Retention", "Partner-Led"]),
  productLine: z.string().min(2),
  defaultTargetRoles: z.array(z.enum(BUYING_ROLES)).min(1),
  defaultICP: z.object({
    preferredVerticals: z.array(z.enum(VERTICALS)).optional().default([]),
    preferredRegions: z.array(z.enum(REGIONS)).optional().default([]),
    preferredSegments: z.array(z.enum(ACCOUNT_SEGMENTS)).optional().default([]),
    preferredMotions: z.array(z.enum(GTM_MOTIONS)).optional().default([]),
    minEmployeeBand: z.enum(["1-50", "51-200", "201-1000", "1001-5000", "5000+"]).optional(),
    minSecurityMaturity: z.enum(["Low", "Moderate", "High", "Very High"]).optional(),
    requiredTech: z.array(z.string()).optional().default([]),
    excludedVerticals: z.array(z.enum(VERTICALS)).optional().default([]),
    excludedRegions: z.array(z.enum(REGIONS)).optional().default([]),
    excludedSignals: z.array(z.string()).optional().default([])
  }).optional().default({}),
  sellerContext: z.object({
    ownerName: z.string().optional().default(""),
    ownerRole: z.string().optional().default(""),
    motionOwner: z.enum(["AE", "SDR", "Founder", "Partner", "CSM", "Marketing"]).optional().default("Marketing"),
    channelsAvailable: z.array(z.enum(["Email", "LinkedIn", "Paid", "Events", "Partner", "Phone"])).optional().default([]),
    promotionTriggerDefinition: z.string().optional().default("")
  }).optional().default({}),
  enrichmentAllowed: z.boolean().optional().default(true)
});

/* -----------------------------
   SCHEMAS
------------------------------ */

const committeeMemberSchema = z.object({
  name: z.string().min(1),
  title: z.string().min(1),
  role: z.enum(BUYING_ROLES),
  seniority: z.enum(["C-Level", "VP", "Director", "Manager", "IC"]).optional().default("Director"),
  influenceScore: z.number().min(0).max(100).optional().default(50),
  relationshipStrength: z.number().min(0).max(100).optional().default(0),
  engaged: z.boolean().optional().default(false)
});

const signalSchema = z.object({
  signalType: z.enum([
    "Bombora Surge",
    "G2 Research",
    "Website Engagement",
    "Demo Request",
    "Pricing Page Visit",
    "Competitor Comparison",
    "Job Hiring Spike",
    "Funding Event",
    "Expansion Event",
    "Exec Change",
    "Partner Motion",
    "Product Usage",
    "Outbound Reply"
  ]),
  strength: z.number().min(0).max(100),
  recencyDays: z.number().int().min(0).max(365).optional().default(30),
  source: z.string().min(1).optional().default("manual")
});

const techSchema = z.object({
  crm: z.enum(["Salesforce", "HubSpot", "Dynamics", "None", "Unknown"]).optional().default("Unknown"),
  map: z.enum(["Marketo", "HubSpot", "Pardot", "Braze", "None", "Unknown"]).optional().default("Unknown"),
  dataWarehouse: z.enum(["Snowflake", "BigQuery", "Redshift", "Databricks", "None", "Unknown"]).optional().default("Unknown"),
  cloud: z.enum(["AWS", "Azure", "GCP", "Hybrid", "Unknown"]).optional().default("Unknown"),
  securityMaturity: z.enum(["Low", "Moderate", "High", "Very High"]).optional().default("Moderate"),
  stackFitScore: z.number().min(0).max(100).optional().default(50)
}).optional().default({});

export const accountSchema = z.object({
  accountName: z.string().min(2),
  vertical: z.enum(VERTICALS),
  region: z.enum(REGIONS),
  segment: z.enum(ACCOUNT_SEGMENTS),

  gtmMotion: z.enum(GTM_MOTIONS).optional().default("Sales-Led"),
  domain: z.string().min(2).optional().default("unknown"),
  employeeBand: z.enum(["1-50", "51-200", "201-1000", "1001-5000", "5000+"]).optional(),
  revenueBand: z.enum(["<10M", "10-50M", "50-250M", "250M-1B", "1B+"]).optional(),
  accountCoverageModel: z.enum(["Named", "Pooled", "Overlay", "Partner"]).optional().default("Named"),
  existingCustomer: z.boolean().optional().default(false),
  expansionCandidate: z.boolean().optional().default(false),
  openOpportunity: z.boolean().optional().default(false),
  strategicNamed: z.boolean().optional().default(false),
  competitorPresent: z.boolean().optional().default(false),
  partnerAvailable: z.boolean().optional().default(false),

  committee: z.array(committeeMemberSchema).optional().default([]),
  signals: z.array(signalSchema).optional().default([]),
  tech: techSchema,

  firstPartyEngagementScore: z.number().min(0).max(100).optional(),
  thirdPartyIntentScore: z.number().min(0).max(100).optional(),
  relationshipScore: z.number().min(0).max(100).optional(),

  currentPipelineStage: z.enum([
    "No Opportunity",
    "Targeting",
    "Engaged",
    "Meeting",
    "Discovery",
    "Evaluation",
    "Procurement",
    "Negotiation",
    "Customer"
  ]).optional().default("No Opportunity"),

  dealPotentialBand: z.enum(["Low", "Medium", "High", "Strategic"]).optional().default("Medium"),
  notes: z.string().optional().default("")
});

export const abmProjectSchema = z.object({
  clientName: z.string().min(2),
  objective: z.enum(["Net New", "Expansion", "Cross-Sell", "Retention", "Partner-Led"]),
  productLine: z.string().min(2),
  targetRoles: z.array(z.enum(BUYING_ROLES)).min(1),
  account: accountSchema,
  icp: z.object({
    preferredVerticals: z.array(z.enum(VERTICALS)).optional().default([]),
    preferredRegions: z.array(z.enum(REGIONS)).optional().default([]),
    preferredSegments: z.array(z.enum(ACCOUNT_SEGMENTS)).optional().default([]),
    preferredMotions: z.array(z.enum(GTM_MOTIONS)).optional().default([]),
    minEmployeeBand: z.enum(["1-50", "51-200", "201-1000", "1001-5000", "5000+"]).optional(),
    minSecurityMaturity: z.enum(["Low", "Moderate", "High", "Very High"]).optional(),
    requiredTech: z.array(z.string()).optional().default([]),
    excludedVerticals: z.array(z.enum(VERTICALS)).optional().default([]),
    excludedRegions: z.array(z.enum(REGIONS)).optional().default([]),
    excludedSignals: z.array(z.string()).optional().default([])
  }).optional().default({}),
  sellerContext: programSetupSchema.shape.sellerContext.optional().default({}),
  programDefaults: z.object({
    enrichmentAllowed: z.boolean().optional().default(true)
  }).optional().default({})
});

export const portfolioSchema = z.object({
  accounts: z.array(abmProjectSchema).min(1).max(100),
  topN: z.number().int().min(1).max(50).optional().default(10)
});

/* -----------------------------
   VALIDATION
------------------------------ */

export function validationErrorsToFieldMap(error) {
  const out = {};
  for (const issue of error.issues) {
    const key = issue.path.length ? issue.path.join(".") : "form";
    if (!out[key]) out[key] = [];
    out[key].push(issue.message);
  }
  return out;
}

/* -----------------------------
   CONFIDENCE + DATA PRESENCE
------------------------------ */

function buildConfidence(project) {
  const account = project.account;
  const hasFitCore =
    Boolean(account.vertical) &&
    Boolean(account.region) &&
    Boolean(account.segment) &&
    Boolean(project.targetRoles?.length);

  const hasEmployee = Boolean(account.employeeBand);
  const hasTech = Boolean(account.tech && (
    account.tech.stackFitScore !== undefined ||
    account.tech.crm !== "Unknown" ||
    account.tech.map !== "Unknown" ||
    account.tech.cloud !== "Unknown"
  ));

  const hasSignals = safeArray(account.signals).length > 0;
  const hasFirstParty = typeof account.firstPartyEngagementScore === "number";
  const hasThirdParty = typeof account.thirdPartyIntentScore === "number";
  const hasCommittee = safeArray(account.committee).length > 0;
  const hasRelationship = typeof account.relationshipScore === "number";

  const fitConfidence = clamp(
    (hasFitCore ? 55 : 0) +
      (hasEmployee ? 15 : 0) +
      (hasTech ? 20 : 0) +
      (project.icp?.preferredVerticals?.length ? 10 : 0),
    0,
    100
  );

  const intentConfidence = clamp(
    (hasSignals ? 45 : 0) +
      (hasFirstParty ? 25 : 0) +
      (hasThirdParty ? 25 : 0) +
      (account.openOpportunity ? 5 : 0),
    0,
    100
  );

  const committeeConfidence = clamp(
    (hasCommittee ? 70 : 0) +
      (hasRelationship ? 20 : 0) +
      (safeArray(account.committee).some((m) => m.engaged) ? 10 : 0),
    0,
    100
  );

  const overallConfidence = Math.round(
    fitConfidence * 0.45 + intentConfidence * 0.30 + committeeConfidence * 0.25
  );

  const missingDataWarnings = [];

  if (!hasSignals && !hasFirstParty && !hasThirdParty) {
    missingDataWarnings.push(
      "No intent or engagement data provided. Recommendations are fit-led, not demand-led."
    );
  }

  if (!hasCommittee) {
    missingDataWarnings.push(
      "No buying committee data provided. Committee coverage is inferred from target roles and marked low-confidence."
    );
  }

  if (!hasEmployee) {
    missingDataWarnings.push(
      "Employee band missing. Firmographic fit is partially inferred."
    );
  }

  if (!hasTech) {
    missingDataWarnings.push(
      "Technographic data missing. Stack-fit scoring is conservative."
    );
  }

  return {
    fitConfidence,
    intentConfidence,
    committeeConfidence,
    overallConfidence,
    missingDataWarnings
  };
}

/* -----------------------------
   SCORING
------------------------------ */

function scoreFit(project) {
  const { account } = project;
  const icp = project.icp || {};

  let score = 0;
  const reasons = [];
  const risks = [];

  if (!icp.preferredVerticals?.length || icp.preferredVerticals.includes(account.vertical)) {
    score += 18;
    reasons.push(`Vertical fit: ${account.vertical}`);
  } else {
    risks.push(`Vertical outside preferred ICP: ${account.vertical}`);
  }

  if (!icp.preferredRegions?.length || icp.preferredRegions.includes(account.region)) {
    score += 12;
    reasons.push(`Regional fit: ${account.region}`);
  } else {
    risks.push(`Region outside preferred ICP: ${account.region}`);
  }

  if (!icp.preferredSegments?.length || icp.preferredSegments.includes(account.segment)) {
    score += 10;
    reasons.push(`Segment fit: ${account.segment}`);
  } else {
    risks.push(`Segment outside preferred ICP: ${account.segment}`);
  }

  if (!icp.preferredMotions?.length || icp.preferredMotions.includes(account.gtmMotion)) {
    score += 6;
    reasons.push(`GTM motion fit: ${account.gtmMotion}`);
  }

  if (icp.minEmployeeBand && account.employeeBand) {
    if (compareOrdered(account.employeeBand, icp.minEmployeeBand, employeeBandOrder)) {
      score += 8;
      reasons.push(`Employee band clears ICP floor: ${account.employeeBand}`);
    } else {
      risks.push(`Employee band below ICP floor: ${account.employeeBand}`);
    }
  } else {
    score += 4;
    reasons.push("Employee band unavailable; partial fit score applied");
  }

  if (icp.minSecurityMaturity && account.tech?.securityMaturity) {
    if (compareOrdered(account.tech.securityMaturity, icp.minSecurityMaturity, maturityOrder)) {
      score += 6;
      reasons.push(`Security maturity acceptable: ${account.tech.securityMaturity}`);
    } else {
      risks.push(`Security maturity below preferred threshold: ${account.tech.securityMaturity}`);
    }
  } else {
    score += 3;
    reasons.push("Security maturity unavailable; conservative baseline applied");
  }

  const stackFit = typeof account.tech?.stackFitScore === "number" ? account.tech.stackFitScore : 50;
  score += stackFit * 0.22;
  reasons.push(`Technographic fit contributes ${Math.round(stackFit * 0.22)} points`);

  if (account.strategicNamed) {
    score += 8;
    reasons.push("Strategic named account");
  }

  if (account.partnerAvailable) {
    score += 4;
    reasons.push("Partner leverage available");
  }

  if (icp.excludedVerticals?.includes(account.vertical)) {
    score -= 25;
    risks.push(`Negative ICP vertical: ${account.vertical}`);
  }

  if (icp.excludedRegions?.includes(account.region)) {
    score -= 20;
    risks.push(`Negative ICP region: ${account.region}`);
  }

  if (account.competitorPresent) {
    score -= 5;
    risks.push("Incumbent competitor present");
  }

  return {
    score: clamp(Math.round(score), 0, 100),
    reasons,
    risks
  };
}

function scoreIntent(account) {
  const signals = safeArray(account.signals);

  const weightedSignals = signals.map((s) => {
    const recencyFactor =
      s.recencyDays <= 7 ? 1 :
      s.recencyDays <= 30 ? 0.8 :
      s.recencyDays <= 90 ? 0.55 :
      0.3;

    const sourceBoost =
      s.signalType === "Demo Request" ? 1.25 :
      s.signalType === "Pricing Page Visit" ? 1.15 :
      s.signalType === "Competitor Comparison" ? 1.15 :
      s.signalType === "Bombora Surge" ? 1.05 :
      1;

    return s.strength * recencyFactor * sourceBoost;
  });

  const signalScore = clamp(avg(weightedSignals), 0, 100);
  const firstParty = typeof account.firstPartyEngagementScore === "number" ? account.firstPartyEngagementScore : null;
  const thirdParty = typeof account.thirdPartyIntentScore === "number" ? account.thirdPartyIntentScore : null;

  let combined;
  if (signals.length || firstParty !== null || thirdParty !== null) {
    combined = clamp(
      Math.round(
        signalScore * 0.40 +
          (thirdParty ?? 0) * 0.35 +
          (firstParty ?? 0) * 0.25
      ),
      0,
      100
    );
  } else {
    combined = 0;
  }

  let stage = "Target";
  let stageConfidence = "low";

  if (combined >= 86) {
    stage = "Purchase";
    stageConfidence = "high";
  } else if (combined >= 70) {
    stage = "Decision";
    stageConfidence = "high";
  } else if (combined >= 50) {
    stage = "Consideration";
    stageConfidence = "medium";
  } else if (combined >= 20) {
    stage = "Awareness";
    stageConfidence = "medium";
  }

  return {
    score: combined,
    stage,
    stageConfidence,
    topSignals: signals
      .slice()
      .sort((a, b) => (b.strength - a.strength) || (a.recencyDays - b.recencyDays))
      .slice(0, 5)
      .map((s) => `${s.signalType} (${s.strength}, ${s.recencyDays}d)`),
    inputCoverage: {
      signalsProvided: signals.length,
      firstPartyProvided: firstParty !== null,
      thirdPartyProvided: thirdParty !== null
    }
  };
}

function scoreCommittee(project) {
  const requiredRoles = unique(project.targetRoles || []);
  const members = safeArray(project.account.committee);
  const presentRoles = unique(members.map((m) => m.role));

  const covered = requiredRoles.filter((r) => presentRoles.includes(r));
  const missing = requiredRoles.filter((r) => !presentRoles.includes(r));

  if (!members.length) {
    return {
      score: 20,
      coveredRoles: [],
      missingRoles: missing,
      relationshipAvg: 0,
      engagedCount: 0,
      inferred: true
    };
  }

  const relationshipAvg = avg(members.map((m) => m.relationshipStrength || 0));
  const engagedCount = members.filter((m) => m.engaged).length;

  let score = 0;
  score += (requiredRoles.length ? covered.length / requiredRoles.length : 0) * 60;
  score += relationshipAvg * 0.25;
  score += Math.min(15, engagedCount * 3);

  return {
    score: clamp(Math.round(score), 0, 100),
    coveredRoles: covered,
    missingRoles: missing,
    relationshipAvg: Math.round(relationshipAvg),
    engagedCount,
    inferred: false
  };
}

function scoreRelationship(account) {
  const direct = typeof account.relationshipScore === "number" ? account.relationshipScore : null;
  const committeeRelationship = avg(safeArray(account.committee).map((m) => m.relationshipStrength || 0));

  if (direct === null && !safeArray(account.committee).length) {
    return {
      score: 15,
      inferred: true
    };
  }

  let score = (direct ?? committeeRelationship) * 0.6 + committeeRelationship * 0.4;

  if (account.existingCustomer && account.expansionCandidate) {
    score += 10;
  }

  return {
    score: clamp(Math.round(score), 0, 100),
    inferred: direct === null
  };
}

function computePenalties(project, intent, committee) {
  const penalties = [];
  let total = 0;

  if (committee.missingRoles.includes("Economic Buyer")) {
    total += 8;
    penalties.push("Economic buyer not mapped");
  }

  if (committee.missingRoles.includes("Technical Buyer")) {
    total += 6;
    penalties.push("Technical buyer not mapped");
  }

  if (project.account.competitorPresent) {
    total += 5;
    penalties.push("Incumbent competitor present");
  }

  if (project.account.currentPipelineStage === "No Opportunity" && intent.stage === "Target") {
    total += 8;
    penalties.push("No active opportunity and weak demand signal");
  }

  if (
    typeof project.account.firstPartyEngagementScore !== "number" &&
    typeof project.account.thirdPartyIntentScore !== "number" &&
    !safeArray(project.account.signals).length
  ) {
    total += 4;
    penalties.push("No intent instrumentation provided");
  }

  return { total, penalties };
}

function decideTier(totalScore, project, intent) {
  if (project.account.strategicNamed && totalScore >= 72) return "Tier 1";
  if (totalScore >= 75 || intent.stage === "Purchase") return "Tier 1";
  if (totalScore >= 58 || intent.stage === "Decision") return "Tier 2";
  return "Tier 3";
}

function buildRegionalOverlay(region) {
  const map = {
    "North America": ["Use ROI-led narrative", "Push urgency with commercial proof"],
    "UKI": ["Emphasize compliance and business-case rigor", "Use lower-hype language"],
    "DACH": ["Lead with architecture, precision, and risk control", "Avoid fluffy claims"],
    "Nordics": ["Use efficient, understated messaging", "Show product credibility"],
    "Southern Europe": ["Use local proof and practical outcomes", "Partner trust can help"],
    "India": ["Balance value, speed, and executive relevance", "Consensus selling matters"],
    "SEA": ["Local maturity varies by market", "Partner and localization leverage matter"],
    "ANZ": ["Tie to operational outcomes", "Show practical transformation gains"],
    "Middle East": ["Trust and executive sponsorship matter", "Use regional proof if available"],
    "LATAM": ["Simplify activation path", "Use partner-assisted execution when possible"],
    "Global": ["Use global narrative with regional adaptation", "Avoid one-size-fits-all execution"]
  };
  return map[region] || ["Use account-specific regional context"];
}

function buildVerticalOverlay(vertical) {
  const map = {
    "Cybersecurity": ["Lead with risk reduction and posture", "Technical buyer enablement is mandatory"],
    "Fintech": ["Stress compliance, resilience, and speed-to-value", "Expect procurement scrutiny"],
    "Healthcare": ["Navigate data sensitivity and operational risk", "Compliance coalition matters"],
    "Pharma": ["Long-cycle stakeholder mapping required", "Procurement/compliance complexity is high"],
    "Logistics": ["Tie value to throughput, SLA, and margin protection", "Operations stakeholder matters"],
    "TravelTech": ["Anchor to demand volatility and yield", "Timing matters"],
    "HRtech": ["Show adoption and workflow simplicity", "Champion role often drives momentum"],
    "Retail": ["Emphasize margin and customer experience", "Planning-cycle timing matters"],
    "Manufacturing": ["Lead with reliability and efficiency", "Technical confidence is critical"],
    "Telecom": ["Expect matrixed buying groups", "Executive sponsor plus technical validation matters"],
    "Media": ["Prove monetization and workflow efficiency", "Narrative testing helps"],
    "Public Sector": ["Procurement and compliance dominate timing", "Stakeholder mapping must be explicit"],
    "Other": ["Use tailored account-specific business case"]
  };
  return map[vertical] || ["Use tailored account-specific business case"];
}

function buildPlays(project, totalScore, intent, committee, tier, confidence) {
  const plays = [];

  if (tier === "Tier 1") {
    plays.push("Run 1:1 account plan with seller + marketing co-ownership");
    plays.push("Deploy bespoke POV tied to account priorities");
  } else if (tier === "Tier 2") {
    plays.push("Run 1:few vertical/region play with tailored proof");
    plays.push("Coordinate SDR + paid + content sequence");
  } else {
    plays.push("Run 1:many nurture with lightweight monitoring");
    plays.push("Promote only if fit or signals improve");
  }

  if (intent.stage === "Purchase") {
    plays.push("Immediate sales activation within 24 hours");
    plays.push("Deliver procurement-ready ROI and migration pack");
  } else if (intent.stage === "Decision") {
    plays.push("Use comparison, ROI, and technical validation assets");
  } else if (intent.stage === "Consideration") {
    plays.push("Use buyer-guide and proof-led education");
  } else {
    plays.push("Use awareness and insight-led air cover");
  }

  if (committee.missingRoles.length) {
    plays.push(`Map missing roles: ${committee.missingRoles.join(", ")}`);
  }

  if (project.account.partnerAvailable) {
    plays.push("Activate partner-assisted credibility motion");
  }

  if (project.account.competitorPresent) {
    plays.push("Prepare displacement narrative and competitive proof");
  }

  if (project.objective === "Expansion" || project.account.expansionCandidate) {
    plays.push("Map whitespace use cases and expansion path");
  }

  if (confidence.intentConfidence < 30) {
    plays.push("Prioritize instrumentation: web engagement, CRM activity, or third-party intent");
  }

  if (confidence.committeeConfidence < 35) {
    plays.push("Do stakeholder mapping before heavy spend");
  }

  if (totalScore < 45) {
    plays.push("Do not overinvest; keep coverage light until fit or demand improves");
  }

  return plays;
}

function buildSummary(project, totalScore, tier, intent, confidence) {
  if (totalScore >= 75) {
    return `${project.clientName} is a strong ${tier} ABM account with credible fit and ${intent.stage.toLowerCase()}-stage buying activity. Overall confidence: ${confidence.overallConfidence}/100.`;
  }
  if (totalScore >= 58) {
    return `${project.clientName} is workable as ${tier}, but execution should stay disciplined because confidence is ${confidence.overallConfidence}/100 and key inputs are incomplete.`;
  }
  return `${project.clientName} is currently lower priority. Keep coverage light until fit, stakeholder access, or demand signals improve. Overall confidence: ${confidence.overallConfidence}/100.`;
}

function buildExecutionPlan(project, tier, intent, confidence) {
  const base = [];

  if (tier === "Tier 1") {
    base.push("Assign clear account owner");
    base.push("Build bespoke POV deck");
    base.push("Map 4-6 stakeholders");
  } else if (tier === "Tier 2") {
    base.push("Run tailored outbound + paid sequence");
    base.push("Use vertical and regional proof");
  } else {
    base.push("Add to monitored nurture");
    base.push("Promote on new signal or stakeholder access");
  }

  if (intent.stage === "Purchase" || intent.stage === "Decision") {
    base.push("Run weekly account review");
  } else {
    base.push("Run fortnightly signal review");
  }

  if (confidence.intentConfidence < 30) {
    base.push("Instrument first-party engagement before scaling spend");
  }

  return base;
}

/* -----------------------------
   PUBLIC ENGINE
------------------------------ */

export function buildAbmProjectIntel(project) {
  const fit = scoreFit(project);
  const intent = scoreIntent(project.account);
  const committee = scoreCommittee(project);
  const relationship = scoreRelationship(project.account);
  const confidence = buildConfidence(project);
  const penalties = computePenalties(project, intent, committee);

  const raw =
    fit.score * 0.42 +
    intent.score * 0.22 +
    committee.score * 0.16 +
    relationship.score * 0.10 +
    (project.account.openOpportunity ? 6 : 0) +
    (project.account.expansionCandidate ? 4 : 0);

  const totalScore = clamp(Math.round(raw - penalties.total), 0, 100);
  const tier = decideTier(totalScore, project, intent);
  const regionalOverlay = buildRegionalOverlay(project.account.region);
  const verticalOverlay = buildVerticalOverlay(project.account.vertical);
  const plays = buildPlays(project, totalScore, intent, committee, tier, confidence);
  const summary = buildSummary(project, totalScore, tier, intent, confidence);
  const executionPlan = buildExecutionPlan(project, tier, intent, confidence);

  return {
    summary,
    totalScore,
    tier,
    buyingStage: intent.stage,
    stageConfidence: intent.stageConfidence,

    scores: {
      fit: fit.score,
      intent: intent.score,
      committeeCoverage: committee.score,
      relationship: relationship.score
    },

    confidence,

    explainability: {
      fitReasons: fit.reasons,
      fitRisks: fit.risks,
      topSignals: intent.topSignals,
      coveredRoles: committee.coveredRoles,
      missingRoles: committee.missingRoles,
      penalties: penalties.penalties,
      missingDataWarnings: confidence.missingDataWarnings
    },

    overlays: {
      regional: regionalOverlay,
      vertical: verticalOverlay
    },

    committee,
    plays,
    executionPlan,
    watchouts: unique([
      ...fit.risks,
      ...penalties.penalties,
      ...confidence.missingDataWarnings
    ])
  };
}

export function prioritizePortfolio({ accounts, topN = 10 }) {
  const ranked = accounts
    .map((project) => {
      const intelligence = buildAbmProjectIntel(project);
      return {
        clientName: project.clientName,
        objective: project.objective,
        productLine: project.productLine,
        accountName: project.account.accountName,
        vertical: project.account.vertical,
        region: project.account.region,
        totalScore: intelligence.totalScore,
        tier: intelligence.tier,
        buyingStage: intelligence.buyingStage,
        overallConfidence: intelligence.confidence.overallConfidence,
        topReasons: intelligence.explainability.fitReasons.slice(0, 3),
        keySignals: intelligence.explainability.topSignals.slice(0, 3),
        missingRoles: intelligence.explainability.missingRoles,
        recommendedPlays: intelligence.plays.slice(0, 4),
        intelligence
      };
    })
    .sort((a, b) => {
      if (b.totalScore !== a.totalScore) return b.totalScore - a.totalScore;
      return b.overallConfidence - a.overallConfidence;
    });

  return {
    summary: {
      totalAccounts: ranked.length,
      tier1: ranked.filter((a) => a.tier === "Tier 1").length,
      tier2: ranked.filter((a) => a.tier === "Tier 2").length,
      tier3: ranked.filter((a) => a.tier === "Tier 3").length
    },
    topAccounts: ranked.slice(0, topN),
    allAccounts: ranked
  };
}