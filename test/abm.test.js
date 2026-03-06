import test from "node:test";
import assert from "node:assert/strict";
import {
  programSetupSchema,
  abmProjectSchema,
  buildAbmProjectIntel,
  prioritizePortfolio
} from "../src/abm.js";

function makeProject(overrides = {}) {
  return {
    clientName: "Test Client",
    objective: "Net New",
    productLine: "Test Product",
    targetRoles: ["Economic Buyer", "Technical Buyer", "Champion"],
    icp: {
      preferredVerticals: ["Fintech", "Healthcare"],
      preferredRegions: ["North America"],
      preferredSegments: ["Enterprise"],
      preferredMotions: ["Sales-Led", "Hybrid"]
    },
    sellerContext: {
      ownerName: "Test Owner",
      ownerRole: "GTM Lead",
      motionOwner: "AE",
      channelsAvailable: ["Email", "LinkedIn"],
      promotionTriggerDefinition: "Promote when fit and intent are credible"
    },
    programDefaults: {
      enrichmentAllowed: true
    },
    account: {
      accountName: "Test Account",
      domain: "example.com",
      vertical: "Fintech",
      region: "North America",
      segment: "Enterprise",
      gtmMotion: "Sales-Led",
      employeeBand: "1001-5000",
      openOpportunity: true,
      currentPipelineStage: "Discovery",
      firstPartyEngagementScore: 50,
      thirdPartyIntentScore: 50,
      relationshipScore: 40,
      signals: [],
      committee: [],
      tech: {
        crm: "Salesforce",
        map: "Marketo",
        dataWarehouse: "Snowflake",
        cloud: "AWS",
        securityMaturity: "High",
        stackFitScore: 70
      }
    },
    ...overrides,
    account: {
      accountName: "Test Account",
      domain: "example.com",
      vertical: "Fintech",
      region: "North America",
      segment: "Enterprise",
      gtmMotion: "Sales-Led",
      employeeBand: "1001-5000",
      openOpportunity: true,
      currentPipelineStage: "Discovery",
      firstPartyEngagementScore: 50,
      thirdPartyIntentScore: 50,
      relationshipScore: 40,
      signals: [],
      committee: [],
      tech: {
        crm: "Salesforce",
        map: "Marketo",
        dataWarehouse: "Snowflake",
        cloud: "AWS",
        securityMaturity: "High",
        stackFitScore: 70
      },
      ...(overrides.account || {})
    }
  };
}

test("program setup schema accepts flexible valid input", () => {
  const parsed = programSetupSchema.safeParse({
    clientName: "Any Client",
    objective: "Net New",
    productLine: "Any Product",
    defaultTargetRoles: ["Economic Buyer", "Technical Buyer"],
    defaultICP: {
      preferredVerticals: ["Fintech"],
      preferredRegions: ["North America"],
      preferredSegments: ["Enterprise"],
      preferredMotions: ["Sales-Led"]
    },
    sellerContext: {
      ownerName: "Any Owner",
      ownerRole: "Any Role",
      motionOwner: "AE",
      channelsAvailable: ["Email"]
    },
    enrichmentAllowed: true
  });

  assert.equal(parsed.success, true);
});

test("abm project schema accepts a generic valid project", () => {
  const parsed = abmProjectSchema.safeParse(makeProject());
  assert.equal(parsed.success, true);
});

test("stronger account scores higher than weaker account", () => {
  const strong = buildAbmProjectIntel(
    makeProject({
      account: {
        signals: [
          { signalType: "Demo Request", strength: 80, recencyDays: 3 },
          { signalType: "Pricing Page Visit", strength: 70, recencyDays: 5 }
        ],
        committee: [
          {
            name: "Buyer One",
            title: "CFO",
            role: "Economic Buyer",
            relationshipStrength: 70,
            engaged: true
          },
          {
            name: "Buyer Two",
            title: "CIO",
            role: "Technical Buyer",
            relationshipStrength: 60,
            engaged: true
          }
        ]
      }
    })
  );

  const weak = buildAbmProjectIntel(
    makeProject({
      account: {
        vertical: "Other",
        region: "LATAM",
        segment: "SMB",
        openOpportunity: false,
        currentPipelineStage: "No Opportunity",
        firstPartyEngagementScore: 0,
        thirdPartyIntentScore: 0,
        relationshipScore: 0,
        signals: [],
        committee: [],
        tech: {
          crm: "Unknown",
          map: "Unknown",
          dataWarehouse: "Unknown",
          cloud: "Unknown",
          securityMaturity: "Low",
          stackFitScore: 20
        }
      },
      icp: {
        preferredVerticals: ["Fintech"],
        preferredRegions: ["North America"],
        preferredSegments: ["Enterprise"],
        preferredMotions: ["Sales-Led"]
      }
    })
  );

  assert.ok(strong.totalScore > weak.totalScore);
});

test("portfolio prioritization sorts higher scored accounts first", () => {
  const goodProject = makeProject({
    account: {
      accountName: "Good Account",
      signals: [{ signalType: "Demo Request", strength: 90, recencyDays: 2 }],
      committee: [
        {
          name: "Exec A",
          title: "CFO",
          role: "Economic Buyer",
          relationshipStrength: 75,
          engaged: true
        }
      ]
    }
  });

  const badProject = makeProject({
    account: {
      accountName: "Bad Account",
      vertical: "Other",
      region: "LATAM",
      segment: "SMB",
      openOpportunity: false,
      currentPipelineStage: "No Opportunity",
      firstPartyEngagementScore: 0,
      thirdPartyIntentScore: 0,
      relationshipScore: 0,
      signals: [],
      committee: [],
      tech: {
        crm: "Unknown",
        map: "Unknown",
        dataWarehouse: "Unknown",
        cloud: "Unknown",
        securityMaturity: "Low",
        stackFitScore: 10
      }
    },
    icp: {
      preferredVerticals: ["Fintech"],
      preferredRegions: ["North America"],
      preferredSegments: ["Enterprise"],
      preferredMotions: ["Sales-Led"]
    }
  });

  const portfolio = prioritizePortfolio({
    accounts: [badProject, goodProject],
    topN: 2
  });

  assert.equal(portfolio.topAccounts.length, 2);
  assert.equal(portfolio.topAccounts[0].accountName, "Good Account");
});