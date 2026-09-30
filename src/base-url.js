// External address of this server, for the OAuth protected-resource metadata and the
// WWW-Authenticate challenge (C-ABM-02).
//
// Rule: when APP_BASE_URL is set it wins, exactly as before. When it is not set, the
// Host and X-Forwarded-Host headers are client-controlled, so they are used only if the
// value is on the ALLOWED_HOSTS list. Anything else gets the fixed default address.

// A host name, an IPv4 address or a bracketed IPv6 address, with an optional port.
// No path, query, userinfo, spaces, tabs, CR or LF can match.
const HOST_PATTERN =
  /^(?:[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?|\[[0-9a-f:.]{2,45}\])(?::[0-9]{1,5})?$/i;

function hostnameOf(hostWithPort) {
  if (hostWithPort.startsWith("[")) return hostWithPort.slice(0, hostWithPort.indexOf("]") + 1);
  const colon = hostWithPort.indexOf(":");
  return colon === -1 ? hostWithPort : hostWithPort.slice(0, colon);
}

// First value of a header that may hold a comma separated list; null when it is not a clean host.
export function cleanHostHeader(value) {
  if (typeof value !== "string") return null;
  const first = value.split(",")[0].trim();
  if (!first || !HOST_PATTERN.test(first)) return null;
  return first.toLowerCase();
}

// "abm.example.com, other.example.com:8443" becomes a Set of clean lower case entries.
// An entry without a port matches that host on any port; an entry with a port matches only that port.
export function parseAllowedHosts(value) {
  const entries = String(value || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => cleanHostHeader(s))
    .filter(Boolean);
  return new Set(entries);
}

export function isAllowedHost(host, allowedHosts) {
  if (!host || !allowedHosts || allowedHosts.size === 0) return false;
  return allowedHosts.has(host) || allowedHosts.has(hostnameOf(host));
}

export function resolveExternalBaseUrl({ appBaseUrl, allowedHosts, defaultBaseUrl, headers }) {
  if (appBaseUrl) return appBaseUrl;

  const host = cleanHostHeader(headers["x-forwarded-host"] || headers.host);
  if (!isAllowedHost(host, allowedHosts)) return defaultBaseUrl;

  // Only http and https are accepted as the scheme; anything else falls back to http, as before.
  const protoHeader = headers["x-forwarded-proto"];
  const proto = typeof protoHeader === "string" ? protoHeader.split(",")[0].trim().toLowerCase() : "";
  return `${proto === "https" ? "https" : "http"}://${host}`;
}
