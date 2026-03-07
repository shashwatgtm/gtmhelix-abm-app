const EXPLORIUM_API_KEY = process.env.EXPLORIUM_API_KEY || "";
const EXPLORIUM_BASE_URL = (
  process.env.EXPLORIUM_BASE_URL || "https://api.explorium.ai"
).replace(/\/+$/, "");
const EXPLORIUM_ENABLED =
  (process.env.EXPLORIUM_ENABLED || "").toLowerCase() === "true";
const EXPLORIUM_TENANT = (process.env.EXPLORIUM_TENANT || "").trim();

const MAX_BULK_ENRICHMENT_COMPANIES = 15;
const DEFAULT_SINGLE_ENRICHMENTS = ["firmographics", "technographics"];

const BUSINESS_ENRICHMENTS = {
  firmographics: {
    slug: "firmographics",
    requiresParameters: false,
    normalize: normalizeFirmographics
  },
  technographics: {
    slug: "technographics",
    requiresParameters: false,
    normalize: normalizeTechnographics
  },
  companySocialMedia: {
    slug: "linkedin_posts",
    requiresParameters: false,
    defaultParameters: { offline_mode: true },
    normalize: normalizeLinkedinPosts
  },
  keywordSearchOnWebsites: {
    slug: "company_website_keywords",
    requiresParameters: true,
    normalize: normalizeWebsiteKeywords,
    validateParameters(parameters) {
      return Array.isArray(parameters?.keywords) && parameters.keywords.length > 0;
    },
    validationMessage:
      "keywordSearchOnWebsites requires parameters.keywords as a non-empty array."
  },
  financialMetrics: {
    slug: "financial_indicators",
    requiresParameters: false,
    normalize: normalizeFinancialMetrics
  },
  fundingAndAcquisitions: {
    slug: "funding_and_acquisition",
    requiresParameters: false,
    normalize: normalizeFundingAndAcquisitions
  },
  businessChallenges: {
    slug: "pc_business_challenges_10k",
    requiresParameters: false,
    normalize: normalizeBusinessChallenges
  },
  competitiveLandscape: {
    slug: "pc_competitive_landscape_10k",
    requiresParameters: false,
    normalize: normalizeCompetitiveLandscape
  },
  strategicInsights: {
    slug: "pc_strategy_10k",
    requiresParameters: false,
    normalize: normalizeStrategicInsights
  },
  webstack: {
    slug: "webstack",
    requiresParameters: false,
    normalize: normalizeWebstack
  },
  companyHierarchy: {
    slug: "company_hierarchy",
    requiresParameters: false,
    normalize: normalizeCompanyHierarchy
  },
  businessWebsiteTraffic: {
    slug: "website_traffic",
    requiresParameters: false,
    normalize: normalizeWebsiteTraffic
  },
  businessIntentTopicsBombora: {
    slug: "bombora_intent",
    requiresParameters: false,
    normalize: normalizeBomboraIntent
  }
};

function isEnabled() {
  return EXPLORIUM_ENABLED && Boolean(EXPLORIUM_API_KEY);
}

function compactObject(obj) {
  return Object.fromEntries(
    Object.entries(obj || {}).filter(([, v]) => v !== undefined && v !== null && v !== "")
  );
}

function normalizeDomain(domain) {
  if (!domain) return undefined;
  let d = String(domain).trim().toLowerCase();
  d = d.replace(/^https?:\/\//, "");
  d = d.replace(/^www\./, "");
  d = d.split("/")[0];
  return d || undefined;
}

function normalizeLinkedinUrl(url) {
  if (!url) return undefined;
  return String(url).trim();
}

function safeLower(v) {
  return v == null ? "" : String(v).toLowerCase();
}

function safeString(v) {
  return v == null ? "" : String(v).trim();
}

function isUnknownLike(v) {
  if (v == null) return true;
  const s = String(v).trim().toLowerCase();
  return s === "" || s === "unknown" || s === "n/a" || s === "na" || s === "none";
}

function firstArrayItem(value) {
  return Array.isArray(value) ? value[0] : value;
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

function uniqueStrings(arr) {
  return [...new Set(asArray(arr).map((x) => safeString(x)).filter(Boolean))];
}

function uniqueObjectsByJson(arr) {
  const seen = new Set();
  const out = [];
  for (const item of asArray(arr)) {
    const key = JSON.stringify(item);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(item);
    }
  }
  return out;
}

function maybeNumber(v) {
  if (v === null || v === undefined || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function unwrapPayload(payload) {
  if (!payload || typeof payload !== "object") return {};

  return (
    firstArrayItem(payload.data) ||
    firstArrayItem(payload.results) ||
    firstArrayItem(payload.records) ||
    firstArrayItem(payload.businesses) ||
    firstArrayItem(payload.enrichments) ||
    payload.result ||
    payload.business ||
    payload.record ||
    payload
  );
}

function flattenStrings(value, out = []) {
  if (value == null) return out;

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    const s = safeString(value);
    if (s) out.push(s);
    return out;
  }

  if (Array.isArray(value)) {
    for (const item of value) flattenStrings(item, out);
    return out;
  }

  if (typeof value === "object") {
    for (const v of Object.values(value)) flattenStrings(v, out);
    return out;
  }

  return out;
}

function pickFirstNonEmpty(obj, keys) {
  for (const key of keys) {
    const value = obj?.[key];
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return undefined;
}

function pickArrayFromKeys(obj, keys) {
  for (const key of keys) {
    const value = obj?.[key];
    if (Array.isArray(value) && value.length) return value;
  }
  return [];
}

function extractTopTextSnippets(data, limit = 5) {
  const snippets = uniqueStrings(flattenStrings(data))
    .filter((s) => s.length >= 20)
    .slice(0, limit);
  return snippets;
}

function detectVendor(rawText, patterns, fallback = undefined) {
  for (const [label, regex] of patterns) {
    if (regex.test(rawText)) return label;
  }
  return fallback;
}

function detectCategoryTerms(rawText, taxonomy) {
  const hits = [];
  for (const [label, patterns] of taxonomy) {
    if (patterns.some((pattern) => pattern.test(rawText))) {
      hits.push(label);
    }
  }
  return hits;
}

async function exploriumFetch(path, body) {
  const headers = {
    "content-type": "application/json",
    api_key: EXPLORIUM_API_KEY
  };

  if (EXPLORIUM_TENANT) {
    headers.tenant = EXPLORIUM_TENANT;
  }

  const res = await fetch(`${EXPLORIUM_BASE_URL}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body)
  });

  const text = await res.text();
  let json = null;

  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(`Explorium request failed: ${res.status}`);
    err.status = res.status;
    err.payload = json;
    err.path = path;
    throw err;
  }

  return json;
}

function pickBusinessMatch(matchResponse) {
  if (!matchResponse || typeof matchResponse !== "object") {
    return { businessId: null, matchedRecord: null };
  }

  const candidateArrays = [
    matchResponse.matched_businesses,
    matchResponse.businesses,
    matchResponse.results,
    matchResponse.records,
    matchResponse.data
  ];

  for (const arr of candidateArrays) {
    if (Array.isArray(arr) && arr.length > 0) {
      const matchedRecord =
        arr.find(
          (x) =>
            x &&
            (x.business_id ||
              x.businessId ||
              x.entity_id ||
              x.id ||
              x.business?.business_id ||
              x.business?.entity_id)
        ) || arr[0];

      const businessId =
        matchedRecord?.business_id ||
        matchedRecord?.businessId ||
        matchedRecord?.entity_id ||
        matchedRecord?.id ||
        matchedRecord?.business?.business_id ||
        matchedRecord?.business?.entity_id ||
        null;

      return {
        businessId,
        matchedRecord: matchedRecord || null
      };
    }
  }

  if (typeof matchResponse.business_id === "string") {
    return { businessId: matchResponse.business_id, matchedRecord: matchResponse };
  }

  if (typeof matchResponse.businessId === "string") {
    return { businessId: matchResponse.businessId, matchedRecord: matchResponse };
  }

  if (typeof matchResponse.entity_id === "string") {
    return { businessId: matchResponse.entity_id, matchedRecord: matchResponse };
  }

  if (typeof matchResponse.id === "string") {
    return { businessId: matchResponse.id, matchedRecord: matchResponse };
  }

  return { businessId: null, matchedRecord: null };
}

function normalizeEmployeeBand(v) {
  if (!v) return undefined;
  const s = safeLower(v);

  if (/(1-10|11-50|1 to 50|1-50)/.test(s)) return "1-50";
  if (/(51-200|51 to 200)/.test(s)) return "51-200";
  if (/(201-1000|201 to 1000)/.test(s)) return "201-1000";
  if (/(1001-5000|1001 to 5000)/.test(s)) return "1001-5000";
  if (/(5000\+|5001|10000|10001|10000\+|10001\+|5000 and above|10000 and above)/.test(s)) {
    return "5000+";
  }

  return undefined;
}

function normalizeRevenueBand(v) {
  if (!v) return undefined;
  const s = safeLower(v);

  if (/(0-500k|0-1m|under 10m|<10m)/.test(s)) return "<10M";
  if (/(10-50m|10m-50m)/.test(s)) return "10-50M";
  if (/(50-250m|50m-250m)/.test(s)) return "50-250M";
  if (/(250m-1b|250-1000m|250m to 1b)/.test(s)) return "250M-1B";
  if (/(1b\+|over 1b|1b and above|1b-10b|10b)/.test(s)) return "1B+";

  return undefined;
}

function normalizeFirmographics(payload) {
  const data = unwrapPayload(payload);

  return compactObject({
    companyName: pickFirstNonEmpty(data, [
      "company_name",
      "business_name",
      "name",
      "display_name"
    ]),
    employeeBand: normalizeEmployeeBand(
      pickFirstNonEmpty(data, [
        "number_of_employees_range",
        "employee_range",
        "employee_band",
        "employees_range",
        "company_size"
      ])
    ),
    revenueBand: normalizeRevenueBand(
      pickFirstNonEmpty(data, [
        "yearly_revenue_range",
        "revenue_range",
        "revenue_band",
        "annual_revenue",
        "company_revenue"
      ])
    ),
    linkedinIndustryCategory: pickFirstNonEmpty(data, [
      "linkedin_industry_category",
      "industry",
      "industry_category",
      "linkedin_industry"
    ]),
    linkedinProfile: pickFirstNonEmpty(data, [
      "linkedin_profile",
      "linkedin_url",
      "linkedin_company_url"
    ]),
    logo: pickFirstNonEmpty(data, ["business_logo", "logo", "logo_url"]),
    countryName: data.country_name,
    regionName: data.region_name,
    cityName: data.city_name,
    website: data.website,
    description: pickFirstNonEmpty(data, [
      "business_description",
      "description",
      "company_description"
    ]),
    ticker: pickFirstNonEmpty(data, ["ticker", "ticker_symbol", "exchange_ticker"]),
    raw: data
  });
}

function normalizeTechnographics(payload) {
  const data = unwrapPayload(payload);
  const rawText = JSON.stringify(data).toLowerCase();

  const crm =
    detectVendor(rawText, [
      ["Salesforce", /salesforce/],
      ["HubSpot", /hubspot/],
      ["Dynamics", /dynamics|microsoft crm/],
      ["Zoho", /zoho/],
      ["Pipedrive", /pipedrive/]
    ]) || data.crm;

  const map =
    detectVendor(rawText, [
      ["Marketo", /marketo/],
      ["Pardot", /pardot|account engagement/],
      ["HubSpot", /hubspot/],
      ["Braze", /braze/],
      ["Eloqua", /eloqua/]
    ]) || data.map;

  const cloud =
    detectVendor(rawText, [
      ["AWS", /\baws\b|amazon web services/],
      ["Azure", /\bazure\b|microsoft azure/],
      ["GCP", /\bgcp\b|google cloud/]
    ]) || data.cloud;

  const fullTechStack = uniqueStrings(
    asArray(
      pickFirstNonEmpty(data, [
        "full_tech_stack",
        "tech_stack",
        "technology_stack"
      ])
    )
  );

  const webstackTech = uniqueStrings(
    asArray(
      pickFirstNonEmpty(data, [
        "technologies_used_by_company_website",
        "webstack",
        "technologies"
      ])
    )
  );

  const dataWarehouse = detectVendor(rawText, [
    ["Snowflake", /snowflake/],
    ["BigQuery", /bigquery/],
    ["Redshift", /redshift/],
    ["Databricks", /databricks/]
  ]);

  const securitySignals = detectCategoryTerms(rawText, [
    ["Identity", [/okta/, /auth0/, /onelogin/]],
    ["Security Monitoring", [/crowdstrike/, /sentinelone/, /splunk/, /datadog/]],
    ["CDN / Edge", [/cloudflare/, /akamai/, /fastly/]]
  ]);

  return compactObject({
    crm,
    map,
    cloud,
    dataWarehouse,
    securitySignals,
    fullTechStack: fullTechStack.slice(0, 100),
    webstackTech: webstackTech.slice(0, 100),
    stackAnchors: uniqueStrings([crm, map, cloud, dataWarehouse, ...securitySignals]).filter(Boolean),
    raw: data
  });
}

function normalizeLinkedinPosts(payload) {
  const postsRaw = Array.isArray(payload?.data)
    ? payload.data
    : Array.isArray(unwrapPayload(payload))
      ? unwrapPayload(payload)
      : asArray(unwrapPayload(payload));

  const posts = postsRaw
    .map((post) =>
      compactObject({
        displayName: pickFirstNonEmpty(post, ["display_name", "author_name", "company_name"]),
        postText: pickFirstNonEmpty(post, ["post_text", "text", "content"]),
        daysSincePosted: maybeNumber(
          pickFirstNonEmpty(post, ["days_since_posted", "daysAgo", "days_since"])
        ),
        postUrl: pickFirstNonEmpty(post, ["post_url", "url", "linkedin_post_url"]),
        numberOfComments: maybeNumber(
          pickFirstNonEmpty(post, ["number_of_comments", "comments_count"])
        ),
        numberOfLikes: maybeNumber(
          pickFirstNonEmpty(post, ["number_of_likes", "likes_count"])
        ),
        createdAt: pickFirstNonEmpty(post, ["created_at", "posted_at", "date"])
      })
    )
    .filter((x) => Object.keys(x).length > 0);

  const textBlob = posts.map((p) => safeLower(p.postText)).join(" ");
  const topics = detectCategoryTerms(textBlob, [
    ["AI", [/\bai\b/, /artificial intelligence/, /machine learning/]],
    ["Product Launch", [/launch/, /announc/, /introduc/]],
    ["Partnership", [/partner/, /alliance/, /ecosystem/]],
    ["Hiring", [/hiring/, /join our team/, /careers/]],
    ["Event", [/webinar/, /summit/, /conference/, /event/]],
    ["Customer Story", [/customer/, /case study/, /success story/]]
  ]);

  const avgLikes =
    posts.length > 0
      ? Math.round(
          posts.reduce((sum, p) => sum + (p.numberOfLikes || 0), 0) / posts.length
        )
      : undefined;

  const avgComments =
    posts.length > 0
      ? Math.round(
          posts.reduce((sum, p) => sum + (p.numberOfComments || 0), 0) / posts.length
        )
      : undefined;

  return compactObject({
    postCount: posts.length,
    latestPostDate: posts[0]?.createdAt,
    latestPostUrl: posts[0]?.postUrl,
    averageLikes: avgLikes,
    averageComments: avgComments,
    dominantTopics: topics,
    recentPosts: posts.slice(0, 5),
    raw: payload
  });
}

function normalizeWebsiteKeywords(payload) {
  const data = unwrapPayload(payload);

  const textResults = uniqueStrings(
    asArray(
      pickFirstNonEmpty(data, [
        "text_results",
        "results",
        "keyword_hits",
        "matched_text"
      ])
    )
  );

  const keywordsIndicator = maybeNumber(
    pickFirstNonEmpty(data, [
      "keywords_indicator",
      "keyword_score",
      "match_score"
    ])
  );

  const matchedKeywords = uniqueStrings(
    asArray(
      pickFirstNonEmpty(data, [
        "matched_keywords",
        "keywords",
        "keyword_list"
      ])
    )
  );

  return compactObject({
    url: pickFirstNonEmpty(data, ["url", "website", "page_url"]),
    keywordsIndicator,
    matchedKeywords,
    hitCount: textResults.length,
    evidenceSnippets: textResults.slice(0, 10),
    raw: data
  });
}

function normalizeFinancialMetrics(payload) {
  const data = unwrapPayload(payload);

  const peerCompanies = uniqueStrings(
    flattenStrings(
      pickFirstNonEmpty(data, [
        "peer_companies",
        "peers",
        "comparable_companies"
      ])
    )
  ).slice(0, 20);

  const leadership = uniqueStrings(
    flattenStrings(
      pickFirstNonEmpty(data, [
        "leadership",
        "leadership_team",
        "executive_team"
      ])
    )
  ).slice(0, 20);

  const profitabilitySignals = [];

  const revenueYearly = maybeNumber(data.revenue_yearly);
  const ebitda = maybeNumber(data.ebitda);
  const cagr = maybeNumber(data.cagr);
  const peRatio = maybeNumber(data.price_earnings_ratio);
  const evToEbitda = maybeNumber(data.enterprise_value_over_ebitda);

  if (revenueYearly !== undefined) profitabilitySignals.push("Revenue available");
  if (ebitda !== undefined) profitabilitySignals.push("EBITDA available");
  if (cagr !== undefined) profitabilitySignals.push("Growth metric available");
  if (peRatio !== undefined) profitabilitySignals.push("Valuation multiple available");
  if (evToEbitda !== undefined) profitabilitySignals.push("EV/EBITDA available");

  return compactObject({
    revenueYearly,
    ebitda,
    cagr,
    priceEarningsRatio: peRatio,
    enterpriseValueOverEbitda: evToEbitda,
    leadership,
    peerCompanies,
    profitabilitySignals,
    raw: data
  });
}

function normalizeFundingAndAcquisitions(payload) {
  const data = unwrapPayload(payload);

  const rounds = asArray(
    pickFirstNonEmpty(data, [
      "funding_rounds",
      "funding",
      "rounds",
      "acquisitions",
      "transactions"
    ])
  );

  const normalizedRounds = rounds
    .map((item) =>
      compactObject({
        type: pickFirstNonEmpty(item, ["type", "transaction_type", "round_type"]),
        amount: maybeNumber(pickFirstNonEmpty(item, ["amount", "deal_amount", "value"])),
        date: pickFirstNonEmpty(item, ["date", "announcement_date", "closed_date"]),
        target: pickFirstNonEmpty(item, ["target", "company", "target_company"]),
        acquirer: pickFirstNonEmpty(item, ["acquirer", "buyer", "acquiring_company"]),
        investors: uniqueStrings(
          flattenStrings(pickFirstNonEmpty(item, ["investors", "participants", "backers"]))
        ).slice(0, 10)
      })
    )
    .filter((x) => Object.keys(x).length > 0);

  const totalKnownFunding = normalizedRounds.reduce(
    (sum, r) => sum + (r.type && /fund/i.test(r.type) && r.amount ? r.amount : 0),
    0
  );

  const acquisitionCount = normalizedRounds.filter(
    (r) => /acqui/i.test(r.type || "")
  ).length;

  const fundingCount = normalizedRounds.filter(
    (r) => /seed|series|fund/i.test(r.type || "")
  ).length;

  return compactObject({
    eventCount: normalizedRounds.length,
    fundingCount,
    acquisitionCount,
    totalKnownFunding: totalKnownFunding || undefined,
    recentEvents: normalizedRounds.slice(0, 10),
    raw: data
  });
}

function normalizeBusinessChallenges(payload) {
  const data = unwrapPayload(payload);
  const textSnippets = extractTopTextSnippets(data, 20);
  const textBlob = textSnippets.join(" ").toLowerCase();

  const challengeThemes = detectCategoryTerms(textBlob, [
    ["Profitability Pressure", [/margin/, /profitability/, /cost/, /expense/]],
    ["Growth Pressure", [/growth/, /expansion/, /market share/, /revenue acceleration/]],
    ["Operational Efficiency", [/efficiency/, /operations/, /automation/, /productivity/]],
    ["Competition", [/competition/, /competitive/, /market pressure/]],
    ["Customer Retention", [/retention/, /churn/, /customer loyalty/]],
    ["Compliance / Regulation", [/regulation/, /compliance/, /regulatory/]],
    ["Cyber / Security", [/security/, /cyber/, /risk/]],
    ["Supply Chain", [/supply chain/, /inventory/, /logistics/]]
  ]);

  return compactObject({
    challengeThemes,
    evidenceSnippets: textSnippets.slice(0, 8),
    raw: data
  });
}

function normalizeCompetitiveLandscape(payload) {
  const data = unwrapPayload(payload);

  const competitorNames = uniqueStrings(
    flattenStrings(
      pickFirstNonEmpty(data, [
        "competitors",
        "peer_companies",
        "competitive_set",
        "named_competitors"
      ])
    )
  ).slice(0, 25);

  const snippets = extractTopTextSnippets(data, 15);
  const textBlob = snippets.join(" ").toLowerCase();

  const landscapeSignals = detectCategoryTerms(textBlob, [
    ["Crowded Market", [/fragmented/, /crowded/, /many competitors/]],
    ["Market Leader Pressure", [/market leader/, /dominant player/, /incumbent/]],
    ["Price Pressure", [/price pressure/, /pricing pressure/, /commodit/]],
    ["Differentiation Challenge", [/differentiat/, /positioning/, /moat/]]
  ]);

  return compactObject({
    competitorCount: competitorNames.length,
    competitorNames,
    landscapeSignals,
    evidenceSnippets: snippets.slice(0, 8),
    raw: data
  });
}

function normalizeStrategicInsights(payload) {
  const data = unwrapPayload(payload);
  const snippets = extractTopTextSnippets(data, 20);
  const textBlob = snippets.join(" ").toLowerCase();

  const strategicThemes = detectCategoryTerms(textBlob, [
    ["AI / Automation", [/artificial intelligence/, /\bai\b/, /automation/, /machine learning/]],
    ["Efficiency", [/efficiency/, /streamline/, /optimization/, /productivity/]],
    ["Expansion", [/expand/, /international/, /new market/, /geograph/]],
    ["Platform / Consolidation", [/platform/, /consolidat/, /unified/, /integrat/]],
    ["Customer Experience", [/customer experience/, /cx/, /customer journey/]],
    ["Security / Risk", [/security/, /risk/, /resilience/, /compliance/]],
    ["Data Strategy", [/data/, /analytics/, /insight/, /warehouse/]]
  ]);

  return compactObject({
    strategicThemes,
    evidenceSnippets: snippets.slice(0, 10),
    raw: data
  });
}

function normalizeWebstack(payload) {
  const data = unwrapPayload(payload);

  const technologiesUsed = uniqueStrings(
    flattenStrings(
      pickFirstNonEmpty(data, [
        "technologies_used_by_company_website",
        "technologies",
        "webstack",
        "full_tech_stack"
      ])
    )
  );

  const categoriesTree = pickFirstNonEmpty(data, [
    "categories_to_technologies_tree",
    "technology_categories",
    "categories"
  ]);

  const textBlob = technologiesUsed.join(" ").toLowerCase();

  const ecommercePlatform = detectVendor(textBlob, [
    ["Shopify", /shopify/],
    ["Magento", /magento|adobe commerce/],
    ["WooCommerce", /woocommerce/],
    ["BigCommerce", /bigcommerce/]
  ]);

  const analyticsStack = detectCategoryTerms(textBlob, [
    ["Google Analytics", [/google analytics/, /\bga4\b/]],
    ["Adobe Analytics", [/adobe analytics/]],
    ["Segment", [/segment/]],
    ["Mixpanel", [/mixpanel/]]
  ]);

  const adStack = detectCategoryTerms(textBlob, [
    ["Google Ads", [/google ads/, /google tag manager/, /gtm/]],
    ["Meta Ads", [/facebook pixel/, /meta pixel/]],
    ["LinkedIn Ads", [/linkedin insight tag/]]
  ]);

  return compactObject({
    companyVertical: pickFirstNonEmpty(data, ["company_vertical", "vertical"]),
    spend: pickFirstNonEmpty(data, ["spend", "estimated_spend"]),
    ecommerce: pickFirstNonEmpty(data, ["ecommerce", "is_ecommerce"]),
    paymentOptions: uniqueStrings(
      flattenStrings(pickFirstNonEmpty(data, ["payment_options", "payments"]))
    ),
    technologiesUsed: technologiesUsed.slice(0, 100),
    technologyCount: technologiesUsed.length,
    ecommercePlatform,
    analyticsStack,
    adStack,
    categoriesToTechnologiesTree: categoriesTree,
    raw: data
  });
}

function normalizeCompanyHierarchy(payload) {
  const data = unwrapPayload(payload);

  const subsidiaries = asArray(
    pickFirstNonEmpty(data, ["subsidiaries", "children", "child_companies"])
  )
    .map((item) =>
      compactObject({
        companyId: pickFirstNonEmpty(item, ["company_id", "id"]),
        companyName: pickFirstNonEmpty(item, ["company_name", "name"]),
        relationshipType: pickFirstNonEmpty(item, ["relationship_type", "type"])
      })
    )
    .filter((x) => Object.keys(x).length > 0);

  return compactObject({
    inputCompanyId: pickFirstNonEmpty(data, ["input_company_id", "company_id"]),
    inputCompanyName: pickFirstNonEmpty(data, ["input_company_name", "company_name"]),
    parentCompanyId: pickFirstNonEmpty(data, ["parent_company_id", "parent_id"]),
    parentCompany: pickFirstNonEmpty(data, ["parent_company", "parent_company_name"]),
    ultimateParentId: pickFirstNonEmpty(data, ["ultimate_parent_id"]),
    ultimateParentName: pickFirstNonEmpty(data, ["ultimate_parent_name"]),
    subsidiaryCount: subsidiaries.length,
    subsidiaries: subsidiaries.slice(0, 25),
    orgTreeJson: data.org_tree_json,
    raw: data
  });
}

function normalizeWebsiteTraffic(payload) {
  const data = unwrapPayload(payload);

  const trafficSeries = asArray(
    pickFirstNonEmpty(data, [
      "monthly_traffic",
      "traffic",
      "visits",
      "traffic_series"
    ])
  )
    .map((item) =>
      compactObject({
        month: pickFirstNonEmpty(item, ["month", "date", "period"]),
        visits: maybeNumber(pickFirstNonEmpty(item, ["visits", "traffic", "value"]))
      })
    )
    .filter((x) => x.month || x.visits !== undefined);

  const latest = trafficSeries[0];
  const peakVisits =
    trafficSeries.length > 0
      ? Math.max(...trafficSeries.map((x) => x.visits || 0))
      : undefined;

  return compactObject({
    latestPeriod: latest?.month,
    latestVisits: latest?.visits,
    peakVisits: peakVisits || undefined,
    monthlyTraffic: trafficSeries.slice(0, 12),
    raw: data
  });
}

function normalizeBomboraIntent(payload) {
  const data = unwrapPayload(payload);

  const topics = asArray(
    pickFirstNonEmpty(data, [
      "intent_topics",
      "topics",
      "bombora_topics"
    ])
  )
    .map((item) => {
      if (typeof item === "string") {
        return { topic: safeString(item) };
      }
      return compactObject({
        topic: pickFirstNonEmpty(item, ["topic", "name", "intent_topic"]),
        score: maybeNumber(pickFirstNonEmpty(item, ["score", "topic_score", "intensity"])),
        levelOfIntent: pickFirstNonEmpty(item, ["level_of_intent", "intent_level"])
      });
    })
    .filter((x) => x.topic);

  const highIntentTopics = topics
    .filter((t) => {
      const score = t.score ?? 0;
      return score >= 60 || /high/i.test(safeString(t.levelOfIntent));
    })
    .slice(0, 10);

  return compactObject({
    levelOfIntent: pickFirstNonEmpty(data, ["level_of_intent", "intent_level"]),
    topicCount:
      maybeNumber(pickFirstNonEmpty(data, ["topic_count"])) || topics.length || undefined,
    intentTopics: topics.slice(0, 25),
    highIntentTopics,
    companyWebsite: pickFirstNonEmpty(data, ["company_website", "website"]),
    companyName: pickFirstNonEmpty(data, ["company_name", "name"]),
    date: pickFirstNonEmpty(data, ["date", "date_stamp", "period"]),
    raw: data
  });
}

function buildDiagnosticsBase() {
  return {
    ok: true,
    attempted: false,
    matched: false,
    enrichmentAvailable: isEnabled(),
    businessId: null,
    reason: "",
    fieldsAdded: [],
    warnings: []
  };
}

function maybePush(arr, value) {
  if (value) arr.push(value);
}

function hasMeaningfulEnrichment(enrichmentObj) {
  if (!enrichmentObj || typeof enrichmentObj !== "object") return false;

  const keys = Object.keys(enrichmentObj).filter((key) => key !== "raw");
  if (keys.length === 0) return false;

  return keys.some((key) => {
    const value = enrichmentObj[key];
    if (value == null) return false;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === "object") return Object.keys(value).length > 0;
    return value !== "";
  });
}

function getEnrichmentDefinition(name) {
  return BUSINESS_ENRICHMENTS[name] || null;
}

function buildSingleEnrichmentPayload(definition, businessId, parameters = {}) {
  const mergedParameters = compactObject({
    ...(definition.defaultParameters || {}),
    ...(parameters || {})
  });

  return compactObject({
    business_id: businessId,
    request_context: null,
    parameters: mergedParameters
  });
}

async function runSingleEnrichment({ enrichmentName, businessId, parameters = {} }) {
  const definition = getEnrichmentDefinition(enrichmentName);

  if (!definition) {
    throw new Error(`Unknown Explorium enrichment: ${enrichmentName}`);
  }

  if (
    definition.requiresParameters &&
    typeof definition.validateParameters === "function" &&
    !definition.validateParameters(parameters)
  ) {
    const err = new Error(
      definition.validationMessage ||
        `Missing required parameters for enrichment "${enrichmentName}".`
    );
    err.status = 422;
    throw err;
  }

  const payload = buildSingleEnrichmentPayload(definition, businessId, parameters);
  const path = `/v1/businesses/${definition.slug}/enrich`;
  const response = await exploriumFetch(path, payload);

  return {
    enrichmentName,
    path,
    payload,
    normalized: definition.normalize(response),
    raw: response
  };
}

export async function enrichBusinessOptional({
  accountName,
  domain,
  linkedinUrl,
  enrichmentNames = DEFAULT_SINGLE_ENRICHMENTS,
  enrichmentParameters = {}
}) {
  const diagnostics = buildDiagnosticsBase();

  if (!isEnabled()) {
    diagnostics.ok = false;
    diagnostics.reason = "Explorium is not configured.";
    diagnostics.warnings.push(
      "EXPLORIUM_ENABLED is false or EXPLORIUM_API_KEY is missing."
    );
    return diagnostics;
  }

  const normalizedDomain = normalizeDomain(domain);
  const normalizedLinkedinUrl = normalizeLinkedinUrl(linkedinUrl);

  if (!accountName || String(accountName).trim().length < 2) {
    diagnostics.ok = false;
    diagnostics.reason =
      "Skipped enrichment because accountName is missing or too short.";
    diagnostics.warnings.push(
      "A usable accountName is required for Explorium matching."
    );
    return diagnostics;
  }

  diagnostics.attempted = true;

  try {
    const businessToMatch = compactObject({
      name: String(accountName).trim(),
      domain: normalizedDomain,
      linkedin_url: normalizedLinkedinUrl,
      url: normalizedDomain ? `https://${normalizedDomain}` : undefined
    });

    const matchRequest = {
      businesses_to_match: [businessToMatch]
    };

    const match = await exploriumFetch("/v1/businesses/match", matchRequest);
    const { businessId, matchedRecord } = pickBusinessMatch(match);

    diagnostics.matchRequest = matchRequest;
    diagnostics.matchSummary = compactObject({
      total_results: match?.total_results,
      total_matches: match?.total_matches
    });
    diagnostics.match = matchedRecord || match || null;

    if (!businessId) {
      diagnostics.reason = "No Explorium business match found.";
      return diagnostics;
    }

    diagnostics.matched = true;
    diagnostics.businessId = businessId;
    diagnostics.reason = "Matched business and attempted enrichment.";

    const requestedEnrichments =
      Array.isArray(enrichmentNames) && enrichmentNames.length
        ? [...new Set(enrichmentNames)]
        : [...DEFAULT_SINGLE_ENRICHMENTS];

    const settlementResults = await Promise.allSettled(
      requestedEnrichments.map((enrichmentName) =>
        runSingleEnrichment({
          enrichmentName,
          businessId,
          parameters: enrichmentParameters?.[enrichmentName] || {}
        })
      )
    );

    diagnostics.enrichments = {};
    diagnostics.enrichmentStatus = {};
    diagnostics.enrichmentRequests = {};

    for (let i = 0; i < requestedEnrichments.length; i += 1) {
      const enrichmentName = requestedEnrichments[i];
      const definition = getEnrichmentDefinition(enrichmentName);
      const settled = settlementResults[i];

      diagnostics.enrichmentStatus[enrichmentName] = settled.status;

      if (settled.status === "fulfilled") {
        diagnostics.enrichments[enrichmentName] = settled.value.normalized;
        diagnostics.enrichmentRequests[enrichmentName] = {
          path: settled.value.path,
          payload: settled.value.payload
        };
      } else {
        diagnostics.enrichments[enrichmentName] = null;
        diagnostics.enrichmentRequests[enrichmentName] = {
          path: definition ? `/v1/businesses/${definition.slug}/enrich` : null,
          payload: definition
            ? buildSingleEnrichmentPayload(
                definition,
                businessId,
                enrichmentParameters?.[enrichmentName] || {}
              )
            : null
        };

        maybePush(
          diagnostics.warnings,
          `${enrichmentName} enrichment unavailable: ${
            settled.reason?.message || "unknown error"
          }`
        );
      }
    }

    diagnostics.firmographics = diagnostics.enrichments.firmographics || null;
    diagnostics.technographics = diagnostics.enrichments.technographics || null;
    diagnostics.companySocialMedia = diagnostics.enrichments.companySocialMedia || null;
    diagnostics.keywordSearchOnWebsites =
      diagnostics.enrichments.keywordSearchOnWebsites || null;
    diagnostics.financialMetrics = diagnostics.enrichments.financialMetrics || null;
    diagnostics.fundingAndAcquisitions =
      diagnostics.enrichments.fundingAndAcquisitions || null;
    diagnostics.businessChallenges = diagnostics.enrichments.businessChallenges || null;
    diagnostics.competitiveLandscape =
      diagnostics.enrichments.competitiveLandscape || null;
    diagnostics.strategicInsights = diagnostics.enrichments.strategicInsights || null;
    diagnostics.webstack = diagnostics.enrichments.webstack || null;
    diagnostics.companyHierarchy = diagnostics.enrichments.companyHierarchy || null;
    diagnostics.businessWebsiteTraffic =
      diagnostics.enrichments.businessWebsiteTraffic || null;
    diagnostics.businessIntentTopicsBombora =
      diagnostics.enrichments.businessIntentTopicsBombora || null;

    const meaningfulEnrichmentCount = Object.values(diagnostics.enrichments).filter(
      (value) => hasMeaningfulEnrichment(value)
    ).length;

    if (meaningfulEnrichmentCount === 0) {
      diagnostics.reason =
        "Business matched, but enrichment returned no usable fields.";
    }

    return diagnostics;
  } catch (error) {
    diagnostics.ok = false;
    diagnostics.matched = false;
    diagnostics.reason =
      "Explorium enrichment failed; continuing without enrichment.";
    diagnostics.error = {
      message: error?.message || "Unknown Explorium error.",
      status: error?.status || null,
      path: error?.path || null,
      payload: error?.payload || null
    };
    maybePush(
      diagnostics.warnings,
      error?.status
        ? `Explorium request failed with status ${error.status}.`
        : "Explorium request failed before completion."
    );
    return diagnostics;
  }
}

export async function enrichBusinessesBulkOptional({
  businesses,
  enrichmentName,
  parameters = {}
}) {
  const response = {
    ok: true,
    attempted: false,
    enrichmentAvailable: isEnabled(),
    enrichmentName,
    requestedCount: Array.isArray(businesses) ? businesses.length : 0,
    matchedCount: 0,
    bulkRequestedCount: 0,
    unmatched: [],
    warnings: [],
    results: null
  };

  if (!isEnabled()) {
    response.ok = false;
    response.reason = "Explorium is not configured.";
    response.warnings.push(
      "EXPLORIUM_ENABLED is false or EXPLORIUM_API_KEY is missing."
    );
    return response;
  }

  if (!Array.isArray(businesses) || businesses.length === 0) {
    response.ok = false;
    response.reason = "Bulk enrichment requires a non-empty businesses array.";
    return response;
  }

  const definition = getEnrichmentDefinition(enrichmentName);
  if (!definition) {
    response.ok = false;
    response.reason = `Unknown Explorium enrichment: ${enrichmentName}`;
    return response;
  }

  if (businesses.length > MAX_BULK_ENRICHMENT_COMPANIES) {
    response.ok = false;
    response.reason = `Bulk enrichment capped at ${MAX_BULK_ENRICHMENT_COMPANIES} companies per request by internal policy.`;
    response.warnings.push(
      `Requested ${businesses.length} companies; cap is ${MAX_BULK_ENRICHMENT_COMPANIES}.`
    );
    return response;
  }

  response.attempted = true;

  const matchedBusinessIds = [];

  for (const business of businesses) {
    const accountName = business?.accountName || business?.name;
    const domain = business?.domain;
    const linkedinUrl = business?.linkedinUrl;

    try {
      const normalizedBusinessDomain = normalizeDomain(domain);

      const matchPayload = {
        businesses_to_match: [
          compactObject({
            name: String(accountName || "").trim(),
            domain: normalizedBusinessDomain,
            linkedin_url: normalizeLinkedinUrl(linkedinUrl),
            url: normalizedBusinessDomain ? `https://${normalizedBusinessDomain}` : undefined
          })
        ]
      };

      const match = await exploriumFetch("/v1/businesses/match", matchPayload);
      const { businessId } = pickBusinessMatch(match);

      if (businessId) {
        matchedBusinessIds.push(businessId);
      } else {
        response.unmatched.push({
          accountName: accountName || null,
          domain: domain || null,
          reason: "No Explorium business match found."
        });
      }
    } catch (error) {
      response.unmatched.push({
        accountName: accountName || null,
        domain: domain || null,
        reason: error?.message || "Unexpected match failure."
      });
    }
  }

  response.matchedCount = matchedBusinessIds.length;

  if (matchedBusinessIds.length === 0) {
    response.reason = "No matched business IDs were available for bulk enrichment.";
    return response;
  }

  try {
    const bulkPayload = compactObject({
      business_ids: matchedBusinessIds,
      parameters: Object.keys(parameters || {}).length ? parameters : undefined
    });

    const bulkPath = `/v1/businesses/${definition.slug}/bulk_enrich`;
    const bulkRaw = await exploriumFetch(bulkPath, bulkPayload);

    response.bulkRequestedCount = matchedBusinessIds.length;
    response.results = {
      path: bulkPath,
      payload: bulkPayload,
      raw: bulkRaw
    };
    response.reason = "Bulk enrichment completed.";

    return response;
  } catch (error) {
    response.ok = false;
    response.reason = "Bulk enrichment failed.";
    response.error = {
      message: error?.message || "Unknown Explorium bulk error.",
      status: error?.status || null,
      path: error?.path || null,
      payload: error?.payload || null
    };
    maybePush(
      response.warnings,
      error?.status
        ? `Explorium bulk request failed with status ${error.status}.`
        : "Explorium bulk request failed before completion."
    );
    return response;
  }
}

function addFieldAdded(diagnostics, fieldPath) {
  if (!diagnostics || !Array.isArray(diagnostics.fieldsAdded)) return;
  if (!diagnostics.fieldsAdded.includes(fieldPath)) {
    diagnostics.fieldsAdded.push(fieldPath);
  }
}

export function mergeExploriumIntoProject(project, enrichment) {
  if (!project || typeof project !== "object") return project;
  if (!enrichment || enrichment.matched !== true) return project;

  const merged = structuredClone(project);

  if (!merged.account) merged.account = {};
  if (!merged.account.tech) merged.account.tech = {};

  const firmo = enrichment.firmographics || {};
  const techno = enrichment.technographics || {};
  const webstack = enrichment.webstack || {};
  const hierarchy = enrichment.companyHierarchy || {};
  const bombora = enrichment.businessIntentTopicsBombora || {};

  if (firmo.employeeBand && isUnknownLike(merged.account.employeeBand)) {
    merged.account.employeeBand = firmo.employeeBand;
    addFieldAdded(enrichment, "account.employeeBand");
  }

  if (firmo.revenueBand && isUnknownLike(merged.account.revenueBand)) {
    merged.account.revenueBand = firmo.revenueBand;
    addFieldAdded(enrichment, "account.revenueBand");
  }

  if (firmo.linkedinIndustryCategory && isUnknownLike(merged.account.vertical)) {
    merged.account.vertical = firmo.linkedinIndustryCategory;
    addFieldAdded(enrichment, "account.vertical");
  }

  if (firmo.linkedinProfile && isUnknownLike(merged.account.linkedinUrl)) {
    merged.account.linkedinUrl = firmo.linkedinProfile;
    addFieldAdded(enrichment, "account.linkedinUrl");
  }

  if (firmo.logo && isUnknownLike(merged.account.logo)) {
    merged.account.logo = firmo.logo;
    addFieldAdded(enrichment, "account.logo");
  }

  if (firmo.website && isUnknownLike(merged.account.domain)) {
    merged.account.domain = normalizeDomain(firmo.website);
    addFieldAdded(enrichment, "account.domain");
  }

  if (techno.crm && isUnknownLike(merged.account.tech.crm)) {
    merged.account.tech.crm = techno.crm;
    addFieldAdded(enrichment, "account.tech.crm");
  }

  if (techno.map && isUnknownLike(merged.account.tech.map)) {
    merged.account.tech.map = techno.map;
    addFieldAdded(enrichment, "account.tech.map");
  }

  if (techno.cloud && isUnknownLike(merged.account.tech.cloud)) {
    merged.account.tech.cloud = techno.cloud;
    addFieldAdded(enrichment, "account.tech.cloud");
  }

  if (
    !merged.account.tech.dataWarehouse &&
    techno.dataWarehouse
  ) {
    merged.account.tech.dataWarehouse = techno.dataWarehouse;
    addFieldAdded(enrichment, "account.tech.dataWarehouse");
  }

  if (
    !merged.account.tech.webstack &&
    Array.isArray(webstack.technologiesUsed) &&
    webstack.technologiesUsed.length > 0
  ) {
    merged.account.tech.webstack = webstack.technologiesUsed;
    addFieldAdded(enrichment, "account.tech.webstack");
  }

  if (
    !merged.account.parentCompany &&
    hierarchy.parentCompany
  ) {
    merged.account.parentCompany = hierarchy.parentCompany;
    addFieldAdded(enrichment, "account.parentCompany");
  }

  if (
    !merged.account.ultimateParentName &&
    hierarchy.ultimateParentName
  ) {
    merged.account.ultimateParentName = hierarchy.ultimateParentName;
    addFieldAdded(enrichment, "account.ultimateParentName");
  }

  if (
    !merged.account.intentTopics &&
    Array.isArray(bombora.highIntentTopics) &&
    bombora.highIntentTopics.length > 0
  ) {
    merged.account.intentTopics = bombora.highIntentTopics.map((t) => t.topic);
    addFieldAdded(enrichment, "account.intentTopics");
  }

  if (
    typeof merged.account.tech.stackFitScore !== "number" &&
    typeof techno.stackFitScore === "number"
  ) {
    merged.account.tech.stackFitScore = techno.stackFitScore;
    addFieldAdded(enrichment, "account.tech.stackFitScore");
  }

  if (enrichment.fieldsAdded.length === 0) {
    enrichment.reason =
      enrichment.reason ||
      "Matched business, but no project fields were updated because existing values already existed.";
  } else {
    enrichment.reason = `Matched business and added ${enrichment.fieldsAdded.length} field(s).`;
  }

  return merged;
}

export const EXPLORIUM_ENRICHMENT_NAMES = Object.freeze(
  Object.keys(BUSINESS_ENRICHMENTS)
);

export const EXPLORIUM_BUSINESS_ENRICHMENTS = Object.freeze(
  Object.fromEntries(
    Object.entries(BUSINESS_ENRICHMENTS).map(([key, value]) => [
      key,
      {
        slug: value.slug,
        requiresParameters: value.requiresParameters === true
      }
    ])
  )
);

export { MAX_BULK_ENRICHMENT_COMPANIES };