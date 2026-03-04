import { z } from "zod";

export const SEGMENTS = [
  "Midmarket ITeS/Telecom",
  "B2B SaaS Startup (Series A/B+)",
  "GenAI Startup",
  "Vertical SaaS"
];

export const VERTICALS = [
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

export const GEOS = ["US", "UK", "India", "Singapore", "Indonesia", "DACH", "Nordics", "UAE", "APAC", "MENA"];

export const intakeSchema = z
  .object({
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
  })
  .superRefine((val, ctx) => {
    if (val.verticals.includes("Other") && (!val.otherVertical || val.otherVertical.trim().length < 3)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["otherVertical"],
        message: "If you select Other, you must specify the vertical (min 3 chars)."
      });
    }
  });

export function validationErrorsToFieldMap(zodError) {
  const out = {};
  for (const issue of zodError.issues ?? []) {
    const key = (issue.path?.join(".") ?? "form").toString();
    out[key] = out[key] ? [...out[key], issue.message] : [issue.message];
  }
  return out;
}

function roleArchetype(role) {
  const r = String(role || "").toLowerCase();
  if (/(ciso|security|secops|iam)/.test(r)) return "security";
  if (/(cio|cto|vp engineering|head of engineering)/.test(r)) return "tech_exec";
  if (/(data|analytics|ml|ai)/.test(r)) return "data";
  if (/(sales|revops|revenue)/.test(r)) return "sales";
  if (/(marketing|demand|growth)/.test(r)) return "marketing";
  if (/(finance|cfo|procurement)/.test(r)) return "finance";
  if (/(operations|ops|supply)/.test(r)) return "ops";
  if (/(hr|people)/.test(r)) return "hr";
  return "exec";
}

function personaMessage(archetype, intake) {
  const topPain = intake.pains?.[0] || "inefficiency and inconsistent execution";
  const proof = `1-page case study in ${intake.verticals.includes("Other") ? intake.otherVertical : intake.verticals[0]} + 3 measurable outcomes`;
  const common = { proof, cta: "15-minute fit check + 3 account targets to validate ICP" };

  if (archetype === "security") {
    return { pain: topPain, message: `Reduce risk and time-to-detect by standardizing controls around ${intake.productCategory}.`, ...common };
  }
  if (archetype === "tech_exec") {
    return { pain: topPain, message: `Shorten delivery cycles and reduce tech debt with ${intake.productCategory} that plugs into your existing stack.`, ...common };
  }
  if (archetype === "data") {
    return { pain: topPain, message: `Turn messy signals into decisions: operationalize ${intake.productCategory} with clear guardrails and measurable lift.`, ...common };
  }
  if (archetype === "sales") {
    return { pain: topPain, message: `Increase meetings with accounts that can actually buy: tighter ICP + triggered outreach tied to buying signals.`, ...common };
  }
  if (archetype === "marketing") {
    return { pain: topPain, message: `Improve engaged-account and meeting rate with tiered ABM plays that match channels to intent.`, ...common };
  }
  if (archetype === "finance") {
    return { pain: topPain, message: `Control CAC and payback by focusing spend on Tier A/B accounts with explainable scoring.`, ...common };
  }
  if (archetype === "ops") {
    return { pain: topPain, message: `Remove operational bottlenecks with repeatable plays tied to clear triggers and ownership.`, ...common };
  }
  if (archetype === "hr") {
    return { pain: topPain, message: `Reduce hiring friction and cycle time with targeted plays for roles and locations you need most.`, ...common };
  }
  return { pain: topPain, message: "Drive predictable pipeline with an ABM system built around ICP discipline and triggered execution.", ...common };
}

function tierPlays() {
  return {
    A: [
      { day: 1, channel: "LinkedIn", action: "Connection + 1 insight hook", goal: "Open loop" },
      { day: 2, channel: "Email", action: "Personalized problem framing + proof", goal: "Start conversation" },
      { day: 4, channel: "Landing page", action: "1:1 page with ICP proof + CTA", goal: "Convert interest" },
      { day: 6, channel: "Exec air cover", action: "Exec-to-exec note or warm intro", goal: "Increase trust" },
      { day: 8, channel: "Call", action: "Direct meeting ask with 2 agenda bullets", goal: "Book meeting" },
      { day: 10, channel: "Ads/Retargeting", action: "Retarget engaged accounts", goal: "Stay present" },
      { day: 14, channel: "Event/Webinar", action: "Invite to vertical roundtable", goal: "High-intent engagement" }
    ],
    B: [
      { day: 1, channel: "LinkedIn", action: "Connection + short value claim", goal: "Open loop" },
      { day: 3, channel: "Email", action: "Vertical angle + quick CTA", goal: "Start conversation" },
      { day: 7, channel: "Content", action: "Send 1 asset mapped to pain", goal: "Educate" },
      { day: 10, channel: "Retargeting", action: "Retarget clickers", goal: "Reinforce" },
      { day: 14, channel: "Email", action: "Meeting ask + 2 proof points", goal: "Book meeting" }
    ],
    C: [
      { week: 1, channel: "Nurture", action: "Add to nurture stream", goal: "Keep warm" },
      { week: 3, channel: "Rescore", action: "Rescore based on new signals", goal: "Promote to Tier B" },
      { week: 6, channel: "Nurture", action: "Periodic insight + soft CTA", goal: "Stay relevant" }
    ]
  };
}

function signalToAction(intake) {
  const out = {};
  const signals = intake.dataSignalsAvailable || [];
  if (signals.includes("Hiring")) out.Hiring = "Trigger outreach tied to role hires + operational urgency angle.";
  if (signals.includes("Funding")) out.Funding = "Trigger outreach tied to growth targets + speed-to-execution angle.";
  if (signals.includes("Intent")) out.Intent = "Trigger outreach with competitor displacement + buying-stage CTA.";
  if (signals.includes("Technographics")) out.Technographics = "Trigger outreach with integration angle + migration proof.";
  if (signals.includes("Website activity")) out["Website activity"] = "Trigger outreach within 24h with asset aligned to visited page.";
  if (signals.includes("None")) out.None = "ABM will underperform without signals. Prioritize data acquisition before scaling Tier A.";
  return out;
}

function negativeIcp(intake) {
  return [
    "Accounts outside target geos or headcount band.",
    "Accounts with no reachable contacts in at least 2 buying roles.",
    "Accounts with 'None' signals AND no plan to add intent/tech/hiring signals within 30 days.",
    intake.salesCycleDays > 180 ? "Very long cycle accounts without exec sponsor access (high stall risk)." : null,
    intake.salesMotion === "PLG" ? "PLG motion without product telemetry and activation events (no trigger-based plays)." : null
  ].filter(Boolean);
}

export function buildAbmBrief(intake) {
  const verticalLabel = intake.verticals.includes("Other") ? intake.otherVertical : intake.verticals.join(", ");
  const personaMessaging = {};
  for (const role of intake.primaryBuyerRoles || []) {
    personaMessaging[role] = personaMessage(roleArchetype(role), intake);
  }

  return {
    icp: {
      segment: intake.segment,
      verticals: verticalLabel,
      geos: intake.geos,
      headcount: intake.targetHeadcountBand,
      salesMotion: intake.salesMotion,
      primaryBuyers: intake.primaryBuyerRoles
    },
    negativeIcp: negativeIcp(intake),
    positioning: {
      oneLiner: intake.oneLiner,
      pains: intake.pains,
      differentiation: intake.differentiation,
      competitors: intake.competitors
    },
    personaMessaging,
    tierPlays: tierPlays(),
    signalTriggers: signalToAction(intake),
    measurement: [
      "Coverage: % accounts with contacts in 3+ buying roles",
      "Engagement: reply rate + meeting rate by tier",
      "Pipeline: SQL/SQO + influenced pipeline by tier",
      "Efficiency: cost per meeting + time-to-first-meeting"
    ],
    assumptions: [
      "No external facts (funding/stack/news) are asserted unless you provide them. Add enrichment for factual personalization.",
      "Treat scoring as explainable guidance; calibrate weights after 2–3 weeks of responses."
    ]
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function buildScoringTemplate(intake) {
  const weights = { firmographic_fit: 30, pain_urgency: 25, intent_triggers: 25, reachability: 20 };

  const columns = [
    "Account Name",
    "Website/Domain",
    "Geo",
    "Industry/Vertical",
    "Headcount",
    "Buying Committee Roles",
    "Trigger Signals Observed",
    "Firmographic Fit (0-30)",
    "Pain Urgency (0-25)",
    "Intent/Triggers (0-25)",
    "Reachability (0-20)",
    "Total Score (0-100)",
    "Tier (A/B/C)",
    "Top 3 Score Drivers",
    "Top 2 Risks",
    "Next Best Action"
  ];

  const tieringRules = [
    "Tier A: 80–100 (1:1 + exec air cover)",
    "Tier B: 60–79 (1:few by vertical/geo)",
    "Tier C: <60 (nurture/recycle)"
  ];

  const sample = {
    "Account Name": "ExampleCo",
    "Website/Domain": "example.com",
    Geo: intake.geos[0],
    "Industry/Vertical": intake.verticals.includes("Other") ? intake.otherVertical : intake.verticals[0],
    Headcount: intake.targetHeadcountBand,
    "Buying Committee Roles": intake.primaryBuyerRoles.join("; "),
    "Trigger Signals Observed": "Hiring; Website activity",
    "Firmographic Fit (0-30)": 24,
    "Pain Urgency (0-25)": 18,
    "Intent/Triggers (0-25)": 15,
    "Reachability (0-20)": 12,
    "Total Score (0-100)": 69,
    "Tier (A/B/C)": "B",
    "Top 3 Score Drivers": "Geo+Vertical fit; Hiring trigger; 2 reachable roles",
    "Top 2 Risks": "Weak urgency; unclear incumbent",
    "Next Best Action": "Send vertical proof + meeting ask"
  };

  const header = columns.join(",");
  const row = columns.map((c) => csvEscape(sample[c])).join(",");
  const csv = `${header}\n${row}\n`;

  return { weights, columns, tieringRules, sampleRows: [sample], csv };
}
