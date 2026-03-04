import test from "node:test";
import assert from "node:assert/strict";
import { intakeSchema, buildAbmBrief, buildScoringTemplate } from "../src/abm.js";

const base = {
  clientName: "AcmeAI",
  website: "https://acme.example",
  segment: "GenAI Startup",
  verticals: ["Fintech"],
  otherVertical: "",
  geos: ["US"],
  productCategory: "AI assistant for risk teams",
  oneLiner: "We help fintech risk teams cut review time with auditable GenAI workflows.",
  primaryBuyerRoles: ["CIO", "Head of Risk"],
  pains: [
    "Manual review is slow and error-prone across risk workflows.",
    "Audits take too long and evidence is scattered."
  ],
  differentiation: [
    "Auditable workflows with human-in-the-loop approvals and traceability.",
    "Fast deployment with existing systems and minimal change management."
  ],
  competitors: ["CompetitorX"],
  avgContractValueUsd: 25000,
  targetHeadcountBand: "51-200",
  salesMotion: "Sales-led",
  salesCycleDays: 60,
  dataSignalsAvailable: ["Hiring", "Website activity"]
};

test("rejects vague one-liner", () => {
  const parsed = intakeSchema.safeParse({ ...base, oneLiner: "too short" });
  assert.equal(parsed.success, false);
});

test("requires otherVertical when verticals includes Other", () => {
  const parsed = intakeSchema.safeParse({ ...base, verticals: ["Other"], otherVertical: "" });
  assert.equal(parsed.success, false);
});

test("brief includes 10x output modules", () => {
  const parsed = intakeSchema.parse(base);
  const brief = buildAbmBrief(parsed);
  assert.ok(brief.personaMessaging);
  assert.ok(brief.tierPlays);
  assert.ok(brief.signalTriggers);
  assert.ok(Array.isArray(brief.negativeIcp));
});

test("scoring template includes explainability columns", () => {
  const parsed = intakeSchema.parse(base);
  const scoring = buildScoringTemplate(parsed);
  assert.ok(scoring.csv.includes("Top 3 Score Drivers"));
  assert.ok(scoring.csv.includes("Next Best Action"));
});
