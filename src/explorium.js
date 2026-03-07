const EXPLORIUM_API_KEY = process.env.EXPLORIUM_API_KEY || "";
const EXPLORIUM_BASE_URL = (
  process.env.EXPLORIUM_BASE_URL || "https://api.explorium.ai"
).replace(/\/+$/, "");
const EXPLORIUM_ENABLED =
  (process.env.EXPLORIUM_ENABLED || "").toLowerCase() === "true";

function isEnabled() {
  return EXPLORIUM_ENABLED && Boolean(EXPLORIUM_API_KEY);
}

function compactObject(obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([, v]) => v !== undefined && v !== null && v !== "")
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

function isUnknownLike(v) {
  if (v == null) return true;
  const s = String(v).trim().toLowerCase();
  return s === "" || s === "unknown" || s === "n/a" || s === "na" || s === "none";
}

function firstArrayItem(value) {
  return Array.isArray(value) ? value[0] : value;
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

async function exploriumFetch(path, body) {
  const res = await fetch(`${EXPLORIUM_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      api_key: EXPLORIUM_API_KEY
    },
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
  if (/(5000\+|5001|10000|10001|10000\+)/.test(s)) return "5000+";

  return undefined;
}

function normalizeRevenueBand(v) {
  if (!v) return undefined;
  const s = safeLower(v);

  if (/(0-500k|0-1m|under 10m|<10m)/.test(s)) return "<10M";
  if (/(10-50m|10m-50m)/.test(s)) return "10-50M";
  if (/(50-250m|50m-250m)/.test(s)) return "50-250M";
  if (/(250m-1b|250-1000m|250m to 1b)/.test(s)) return "250M-1B";
  if (/(1b\+|over 1b|1b and above)/.test(s)) return "1B+";

  return undefined;
}

function normalizeFirmographics(payload) {
  const data = unwrapPayload(payload);

  return compactObject({
    employeeBand: normalizeEmployeeBand(
      data.number_of_employees_range ||
        data.employee_range ||
        data.employee_band ||
        data.employees_range ||
        data.company_size
    ),
    revenueBand: normalizeRevenueBand(
      data.yearly_revenue_range ||
        data.revenue_range ||
        data.revenue_band ||
        data.annual_revenue ||
        data.company_revenue
    ),
    linkedinIndustryCategory:
      data.linkedin_industry_category ||
      data.industry ||
      data.industry_category ||
      data.linkedin_industry,
    linkedinProfile:
      data.linkedin_profile ||
      data.linkedin_url ||
      data.linkedin_company_url,
    logo: data.business_logo || data.logo || data.logo_url,
    raw: data
  });
}

function detectVendor(rawText, patterns, fallback = undefined) {
  for (const [label, regex] of patterns) {
    if (regex.test(rawText)) return label;
  }
  return fallback;
}

function normalizeTechnographics(payload) {
  const data = unwrapPayload(payload);
  const rawText = JSON.stringify(data).toLowerCase();

  const crm =
    detectVendor(rawText, [
      ["Salesforce", /salesforce/],
      ["HubSpot", /hubspot/],
      ["Dynamics", /dynamics|microsoft crm/]
    ]) || data.crm;

  const map =
    detectVendor(rawText, [
      ["Marketo", /marketo/],
      ["Pardot", /pardot|account engagement/],
      ["HubSpot", /hubspot/],
      ["Braze", /braze/]
    ]) || data.map;

  const cloud =
    detectVendor(rawText, [
      ["AWS", /\baws\b|amazon web services/],
      ["Azure", /\bazure\b|microsoft azure/],
      ["GCP", /\bgcp\b|google cloud/]
    ]) || data.cloud;

  return compactObject({
    crm,
    map,
    cloud,
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

  return Boolean(
    enrichmentObj.employeeBand ||
      enrichmentObj.revenueBand ||
      enrichmentObj.linkedinIndustryCategory ||
      enrichmentObj.linkedinProfile ||
      enrichmentObj.logo ||
      enrichmentObj.crm ||
      enrichmentObj.map ||
      enrichmentObj.cloud
  );
}

export async function enrichBusinessOptional({ accountName, domain, linkedinUrl }) {
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

    const [firmographicsResult, technographicsResult] = await Promise.allSettled([
      exploriumFetch("/v1/businesses/enrichments/firmographics", {
        business_id: businessId
      }),
      exploriumFetch("/v1/businesses/enrichments/technographics", {
        business_id: businessId
      })
    ]);

    const firmographics =
      firmographicsResult.status === "fulfilled"
        ? normalizeFirmographics(firmographicsResult.value)
        : null;

    const technographics =
      technographicsResult.status === "fulfilled"
        ? normalizeTechnographics(technographicsResult.value)
        : null;

    diagnostics.firmographics = firmographics;
    diagnostics.technographics = technographics;
    diagnostics.enrichmentStatus = {
      firmographics: firmographicsResult.status,
      technographics: technographicsResult.status
    };

    if (firmographicsResult.status === "rejected") {
      maybePush(
        diagnostics.warnings,
        `Firmographics enrichment unavailable: ${
          firmographicsResult.reason?.message || "unknown error"
        }`
      );
    }

    if (technographicsResult.status === "rejected") {
      maybePush(
        diagnostics.warnings,
        `Technographics enrichment unavailable: ${
          technographicsResult.reason?.message || "unknown error"
        }`
      );
    }

    const hasFirmographics = hasMeaningfulEnrichment(firmographics);
    const hasTechnographics = hasMeaningfulEnrichment(technographics);

    if (!hasFirmographics && !hasTechnographics) {
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