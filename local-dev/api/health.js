const { json, requiredEnv, toIsoNow } = require('./_supabase');

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    return json(res, 405, { ok: false, message: 'Method tidak didukung.' });
  }

  const env = requiredEnv();
  return json(res, 200, {
    ok: true,
    service: 'ess-trendhorizone-api',
    supabase_ready: env.ok,
    timestamp: toIsoNow()
  });
};
