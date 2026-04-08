function json(res, status, data) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(data));
}

function requiredEnv() {
  const url = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || '';
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
  if (!url || !serviceRole) {
    return { ok: false, error: 'SUPABASE_URL atau SUPABASE_SERVICE_ROLE_KEY belum diset.' };
  }
  return { ok: true, url: String(url).replace(/\/+$/, ''), serviceRole };
}

async function supabaseInsert(table, payload) {
  const env = requiredEnv();
  if (!env.ok) return { ok: false, error: env.error };

  const response = await fetch(env.url + '/rest/v1/' + table, {
    method: 'POST',
    headers: {
      apikey: env.serviceRole,
      Authorization: 'Bearer ' + env.serviceRole,
      'Content-Type': 'application/json',
      Prefer: 'return=representation'
    },
    body: JSON.stringify(payload)
  });

  const text = await response.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch (err) {
    body = text;
  }

  if (!response.ok) {
    return { ok: false, status: response.status, error: body };
  }

  return { ok: true, data: body };
}

function toIsoNow() {
  return new Date().toISOString();
}

function randomId(prefix) {
  const rand = Math.random().toString(36).slice(2, 10);
  const ts = Date.now().toString(36);
  return prefix + '_' + ts + rand;
}

module.exports = {
  json,
  requiredEnv,
  supabaseInsert,
  toIsoNow,
  randomId
};
