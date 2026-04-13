const localHandler = require('../local-dev/api/[[...route]].js');

function hasLocalSupabaseEnv() {
  const url = String(process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || '').trim();
  const key = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
  return !!(url && key);
}

function routeFromReq(req) {
  const q = req && req.query ? req.query : {};
  let r = q.route;
  if (Array.isArray(r)) r = r.join('/');
  r = String(r || '').replace(/^\/+/, '');
  if (r) return r;
  const u = String(req.url || '');
  const path = u.split('?')[0] || '';
  return path.replace(/^\/api\/?/, '');
}

async function readRawBody(req) {
  if (req.body !== undefined) {
    if (typeof req.body === 'string' || Buffer.isBuffer(req.body)) return req.body;
    try { return JSON.stringify(req.body || {}); } catch (_) { return ''; }
  }
  return await new Promise(function(resolve, reject) {
    let raw = '';
    req.on('data', function(chunk) { raw += chunk; });
    req.on('end', function() { resolve(raw); });
    req.on('error', reject);
  });
}

module.exports = async function handler(req, res) {
  if (hasLocalSupabaseEnv()) {
    return localHandler(req, res);
  }

  const canonical = String(process.env.CANONICAL_API_BASE || 'https://ess-2026-trendhorizone-id.vercel.app').replace(/\/+$/, '');
  const route = routeFromReq(req);
  const qsIndex = String(req.url || '').indexOf('?');
  const rawQs = qsIndex >= 0 ? String(req.url || '').slice(qsIndex + 1) : '';
  const passthroughQs = rawQs.split('&').filter(function(p) { return p && !p.startsWith('route='); }).join('&');
  const target = canonical + '/api/' + route + (passthroughQs ? ('?' + passthroughQs) : '');

  const headers = Object.assign({}, req.headers || {});
  delete headers.host;
  const method = String(req.method || 'GET').toUpperCase();
  const body = (method === 'GET' || method === 'HEAD') ? undefined : await readRawBody(req);

  try {
    const upstream = await fetch(target, { method: method, headers: headers, body: body });
    res.statusCode = upstream.status;
    upstream.headers.forEach(function(v, k) {
      if (k.toLowerCase() === 'transfer-encoding') return;
      res.setHeader(k, v);
    });
    const txt = await upstream.text();
    res.end(txt);
  } catch (e) {
    res.statusCode = 502;
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.end(JSON.stringify({ ok: false, message: 'Gagal proxy ke API canonical.', error: String((e && e.message) || e || '') }));
  }
};
