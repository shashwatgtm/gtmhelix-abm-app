const EXPLORIUM_API_KEY = process.env.EXPLORIUM_API_KEY || "";
const EXPLORIUM_BASE_URL = (process.env.EXPLORIUM_BASE_URL || "https://api.explorium.ai").replace(/\/+$/, "");
const EXPLORIUM_ENABLED = (process.env.EXPLORIUM_ENABLED || "").toLowerCase() === "true";

function isEnabled() {
  return EXPLORIUM_ENABLED && Boolean(EXPLORIUM_API_KEY);
}

async function exploriumFetch(path, body) {
  const res = await fetch(`${EXPLORIUM_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "api_key": EXPLORIUM_API_KEY
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

function pickBusinessId(matchResponse) {
  if (!matchResponse || typeof matchResponse !== "object") return null;

  if (typeof matchResponse.business_id === "string") return matchResponse.business_id;
  if (typeof matchResponse.entity_id === "string") return matchResponse.entity_id;

  if (Array.isArray(matchResponse.data) && matchResponse.data[0]) {
    return (
      matchResponse.data[0].business_id ||
      matchResponse.data[0].entity_id ||
      null
    );
  }

  if (Array.isArray(matchResponse.businesses) && matchResponse.businesses[0]) {
    return (
      matchResponse.businesses[0].business_id ||
      matchResponse.businesses[0].entity_id ||
      null
    );
  }

  return null;
}

function normalizeEmployeeBand(v) {
  if (!v) return undefined;
  const s = String(v).toLowerCase();

  if (/(1-10|11-50|1 to 50|1-50)/.test(s)) return "1-50";
  if (/(51-200|51 to 200)/.test(s)) return "51-200";
  if (/(201-1000|201 to 1000)/.test(s)) return "201-1000";
  if (/(1001-5000|1001 to 5000)/.test(s)) return "1001-5000";
  if (/(5000\+|5001|10000|10001)/.test(s)) return "5000+";

  return undefined;
}

function normalizeRevenueBand(v) {
  if (!v) return undefined;
  const s = String(v).toLowerCase();

  if (/(0-500k|0-1m|under 10m|<10m)/.test(s)) return "<10M";
  if (/(10-50m|10m-50m)/.test(s)) return "10-50M";
  if (/(50-250m|50m-250m)/.test(s)) return "50-250M";
  if (/(250m-1b|250-1000m|250m to 1b)/.test(s)) return "250M-1B";
  if (/(1b\+|over 1b|1b and above)/.test(s)) return "1B+";

  return undefined;
}

function normalizeFirmographics(payload) {
  const data =
    payload?.data?.[0] ||
    payload?.data ||
    payload?.result ||
    payload ||
    {};

  return {
    employeeBand: normalizeEmployeeBand(data.number_of_employees_range || data.employee_range),
    revenueBand: normalizeRevenueBand(data.yearly_revenue_range || data.revenue_range),
    linkedinIndustryCategory: data.linkedin_industry_category || data.industry || undefined,
    linkedinProfile: data.linkedin_profile || undefined,
    logo: data.business_logo || undefined,
    raw: data
  };
}

function normalizeTechnographics(payload) {
  const data =
    payload?.data?.[0] ||
    payload?.data ||
    payload?.result ||
    payload ||
    {};

  const rawText = JSON.stringify(data).toLowerCase();

  const crm =
    rawText.includes("salesforce") ? "Salesforce" :
    rawText.includes("hubspot") ? "HubSpot" :
    rawText.includes("dynamics") ? "Dynamics" :
    "Unknown";

  const map =
    rawText.includes("marketo") ? "Marketo" :
    rawText.includes("pardot") ? "Pardot" :
    rawText.includes("hubspot") ? "HubSpot" :
    rawText.includes("braze") ? "Braze" :
    "Unknown";

  const cloud =
    rawText.includes("aws") ? "AWS" :
    rawText.includes("azure") ? "Azure" :
    rawText.includes("gcp") || rawText.includes("google cloud") ? "GCP" :
    "Unknown";

  return {
    crm,
    map,
    cloud,
    stackFitScore: 65,
    raw: data
  };
}

export async function enrichBusinessOptional({ accountName, domain, linkedinUrl }) {
  if (!isEnabled()) {
    return {
      ok: false,
      matched: false,
      enrichmentAvailable: false,
      warning: "Explorium is not configured."
    };
  }

  try {
    const matchPayload = {
      company_name: accountName,
      domain,
      linkedin_profile: linkedinUrl
    };

    const match = await exploriumFetch("/businesses/match_businesses", matchPayload);
    const businessId = pickBusinessId(match);

    if (!businessId) {
      return {
        ok: true,
        matched: false,
        enrichmentAvailable: true,
        warning: "No Explorium business match found.",
        match
      };
    }

    const [firmographics, technographics] = await Promise.allSettled([
      exploriumFetch("/businesses/enrichments/firmographics", { business_id: businessId }),
      exploriumFetch("/businesses/enrichments/technographics", { business_id: businessId })
    ]);

    return {
      ok: true,
      matched: true,
      enrichmentAvailable: true,
      businessId,
      firmographics:
        firmographics.status === "fulfilled"
          ? normalizeFirmographics(firmographics.value)
          : null,
      technographics:
        technographics.status === "fulfilled"
          ? normalizeTechnographics(technographics.value)
          : null,
      warnings: [
        ...(firmographics.status === "rejected" ? ["Firmographics enrichment unavailable."] : []),
        ...(technographics.status === "rejected" ? ["Technographics enrichment unavailable."] : [])
      ]
    };
  } catch (error) {
    return {
      ok: false,
      matched: false,
      enrichmentAvailable: true,
      warning: "Explorium enrichment failed; continuing without enrichment.",
      error: {
        message: error.message,
        status: error.status || null
      }
    };
  }
}

export function mergeExploriumIntoProject(project, enrichment) {
  if (!enrichment || !enrichment.matched) return project;

  const merged = structuredClone(project);

  if (enrichment.firmographics?.employeeBand && !merged.account.employeeBand) {
    merged.account.employeeBand = enrichment.firmographics.employeeBand;
  }

  if (enrichment.firmographics?.revenueBand && !merged.account.revenueBand) {
    merged.account.revenueBand = enrichment.firmographics.revenueBand;
  }

  if (enrichment.technographics) {
    merged.account.tech = {
      ...(merged.account.tech || {}),
      ...(enrichment.technographics.crm ? { crm: enrichment.technographics.crm } : {}),
      ...(enrichment.technographics.map ? { map: enrichment.technographics.map } : {}),
      ...(enrichment.technographics.cloud ? { cloud: enrichment.technographics.cloud } : {}),
      ...(typeof enrichment.technographics.stackFitScore === "number"
        ? { stackFitScore: enrichment.technographics.stackFitScore }
        : {})
    };
  }

  return merged;
}