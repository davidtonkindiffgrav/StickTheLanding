// Worker API for the index.html homepage app-shell.
//
// Replaces the old approach (ship the whole stick_WAG.db/stick_MAG.db file to
// the browser via sql.js and query it in-memory) with a single generic,
// read-only query endpoint per sport backed by D1. index.html's existing SQL
// query strings are sent here unchanged - only the transport moved, not the
// queries themselves - so this file has to stay compatible with whatever
// shape those queries expect back (see maskRows/exec response below).
//
// Not a general-purpose SQL API: only single SELECT statements are accepted,
// and every response is scrubbed for redacted athletes before it leaves this
// Worker (see maskRows) since D1 stores the real, unmasked name at rest -
// masking here is what replaces the old client-side UPDATE-on-load trick,
// which only worked because that was a disposable in-browser copy.

const SPORT_BINDINGS = { wag: "DB_WAG", mag: "DB_MAG" };

const BLOCKED_KEYWORDS =
  /\b(insert|update|delete|drop|alter|create|attach|detach|pragma|replace|vacuum|reindex|begin|commit)\b/i;

function sanitizeSelect(sql) {
  const trimmed = sql.trim().replace(/;\s*$/, "");
  if (trimmed.includes(";")) return null;
  if (!/^select\b/i.test(trimmed)) return null;
  if (BLOCKED_KEYWORDS.test(trimmed)) return null;
  return trimmed;
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

function maskName(name) {
  return String(name).trim().split(/\s+/).map((w) => "_".repeat(w.length)).join(" ");
}

// Small in-isolate cache, one entry per sport - the redacted list changes
// rarely (redact_athlete.py is run by hand) so a short TTL just saves a
// second D1 round trip on every request without risking meaningful staleness.
const redactedCache = { wag: { set: null, ts: 0 }, mag: { set: null, ts: 0 } };
const REDACTED_TTL_MS = 60_000;

async function getRedactedNames(sport, db) {
  const entry = redactedCache[sport];
  const now = Date.now();
  if (entry.set && now - entry.ts < REDACTED_TTL_MS) return entry.set;
  const { results } = await db.prepare("SELECT name FROM athletes WHERE redacted = 1").all();
  entry.set = new Set(results.map((r) => r.name));
  entry.ts = now;
  return entry.set;
}

// Masks any row that names a redacted athlete, either directly (an
// athlete/name column holding that name) or indirectly (the query was bound
// to a redacted name, e.g. "WHERE name = ?", even though this particular
// SELECT doesn't re-select the name column - nickname/image lookups do this).
function maskRows(rows, params, redactedSet) {
  const paramRedacted = params.some((p) => typeof p === "string" && redactedSet.has(p));
  return rows.map((row) => {
    const out = { ...row };
    let rowRedacted = paramRedacted;
    for (const key of ["athlete", "name"]) {
      if (typeof out[key] === "string" && redactedSet.has(out[key])) {
        out[key] = maskName(out[key]);
        rowRedacted = true;
      }
    }
    if (rowRedacted) {
      if ("nickname" in out) out.nickname = null;
      if ("image" in out) out.image = null;
    }
    return out;
  });
}

async function handleQuery(request, env, sport) {
  if (request.method !== "POST") return jsonResponse({ error: "Method not allowed" }, 405);

  let body;
  try {
    body = await request.json();
  } catch (_) {
    return jsonResponse({ error: "Invalid JSON body" }, 400);
  }

  const sql = body && body.sql;
  const params = Array.isArray(body && body.params) ? body.params : [];

  if (typeof sql !== "string" || !sql.trim()) return jsonResponse({ error: "Missing sql" }, 400);
  const safeSql = sanitizeSelect(sql);
  if (!safeSql) return jsonResponse({ error: "Only a single SELECT statement is allowed" }, 400);
  if (!params.every((p) => p === null || ["string", "number", "boolean"].includes(typeof p))) {
    return jsonResponse({ error: "Invalid params" }, 400);
  }

  const db = env[SPORT_BINDINGS[sport]];
  try {
    const { results } = await db.prepare(safeSql).bind(...params).all();
    const redacted = await getRedactedNames(sport, db);
    const masked = maskRows(results, params, redacted);
    const columns = masked.length ? Object.keys(masked[0]) : [];
    const values = masked.map((row) => columns.map((c) => row[c]));
    // Shape matches sql.js's Database.exec() return value, since that's what
    // index.html's _execRows()/DB.exec() call sites already expect.
    return jsonResponse([{ columns, values }]);
  } catch (e) {
    return jsonResponse({ error: "Query failed: " + (e.message || String(e)) }, 500);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/api\/(wag|mag)\/query$/i);
    if (match) return handleQuery(request, env, match[1].toLowerCase());
    return env.ASSETS.fetch(request);
  },
};
