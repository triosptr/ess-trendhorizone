const crypto = require('crypto');

function json(res, status, data) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(data));
}

async function readBody(req) {
  if (req.body !== undefined) {
    if (typeof req.body === 'string') return JSON.parse(req.body || '{}');
    return req.body || {};
  }
  return await new Promise(function(resolve, reject) {
    let raw = '';
    req.on('data', function(chunk) { raw += chunk; });
    req.on('end', function() {
      if (!raw) return resolve({});
      try { resolve(JSON.parse(raw)); } catch (err) { reject(err); }
    });
    req.on('error', reject);
  });
}

function env() {
  const url = String(process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || '').replace(/\/+$/, '');
  const key = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
  if (!url || !key) return null;
  return { url: url, key: key };
}

function buildQuery(params) {
  const qs = new URLSearchParams();
  Object.entries(params || {}).forEach(function(entry) {
    if (entry[1] !== undefined && entry[1] !== null && entry[1] !== '') qs.append(entry[0], String(entry[1]));
  });
  const str = qs.toString();
  return str ? '?' + str : '';
}

async function db(method, table, params, body, extraHeaders) {
  const e = env();
  if (!e) return { ok: false, error: 'SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY belum diset.' };
  const headers = Object.assign({
    apikey: e.key,
    Authorization: 'Bearer ' + e.key,
    'Content-Type': 'application/json'
  }, extraHeaders || {});
  const response = await fetch(e.url + '/rest/v1/' + table + buildQuery(params), {
    method: method,
    headers: headers,
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (err) { data = text; }
  if (!response.ok) return { ok: false, status: response.status, error: data };
  return { ok: true, status: response.status, data: data };
}

function nowIso() { return new Date().toISOString(); }
function isValidEmail(email) { return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || '').trim().toLowerCase()); }
function isValidDateYmd(value) { return /^\d{4}-\d{2}-\d{2}$/.test(String(value || '').trim()); }
function isBooleanLike(value) {
  const v = String(value).toLowerCase();
  return v === 'true' || v === 'false' || value === true || value === false;
}
function wibParts(dateObj) {
  const d = dateObj instanceof Date ? dateObj : new Date();
  const dtf = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Jakarta',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  const p = dtf.formatToParts(d).reduce(function(acc, x) { if (x && x.type) acc[x.type] = x.value; return acc; }, {});
  return {
    year: String(p.year || '1970'),
    month: String(p.month || '01'),
    day: String(p.day || '01'),
    hour: String(p.hour || '00'),
    minute: String(p.minute || '00'),
    second: String(p.second || '00')
  };
}
function ymd() {
  const p = wibParts(new Date());
  return p.year + '-' + p.month + '-' + p.day;
}
function hms() {
  const p = wibParts(new Date());
  return p.hour + ':' + p.minute + ':' + p.second;
}
function rid(prefix) { return prefix + '_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }
function roleAdmin(role) { const v = String(role || '').toLowerCase(); return v === 'superadmin' || v === 'admin' || v === 'manager'; }
function hashSha256(v) { return crypto.createHash('sha256').update(String(v || '')).digest('hex'); }
function authCredKey(email) { return 'AUTH_CRED_' + hashSha256(String(email || '').trim().toLowerCase()).slice(0, 24); }
function authSessionKey(token) { return 'AUTH_SESSION_' + String(token || '').trim(); }
function profileExtraKey(employeeId) { return 'PROFILE_EXTRA_' + String(employeeId || '').trim(); }
function randomPassword(len) {
  const size = Math.max(8, Number(len || 10));
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789@#%';
  let out = '';
  for (let i = 0; i < size; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}
async function deliverActivationEmail(toEmail, employeeName, passwordPlain, options) {
  const opt = options || {};
  const to = String(toEmail || '').trim().toLowerCase();
  if (!to) return { sent: false, channel: 'none', message: 'Email tujuan kosong.', debug: { to: to } };
  const apiKey = String(process.env.RESEND_API_KEY || process.env.RESEND_KEY || process.env.EMAIL_RESEND_API_KEY || '').trim();
  const from = String(process.env.RESEND_FROM || 'ESS Trendhorizone <no-reply@trendhorizone.space>').trim();
  const fallbackFrom = String(process.env.RESEND_FROM_FALLBACK || 'Trendhorizone ESS <onboarding@resend.dev>').trim();
  const appUrl = String(process.env.APP_BASE_URL || 'https://ess-trendhorizone-id.vercel.app').trim().replace(/\/+$/,'');
  let autoFrom = '';
  try {
    const host = String(new URL(appUrl).hostname || '').trim().toLowerCase();
    if (host) autoFrom = 'ESS Trendhorizone <no-reply@' + host + '>';
  } catch (_) { autoFrom = ''; }
  const username = String(opt.username || to).trim().toLowerCase();
  const mode = String(opt.mode || 'activation').toLowerCase();
  const userRole = String(opt.role || 'employee').trim().toLowerCase();
  const isAdminRecipient = /^(admin|superadmin|manager)$/.test(userRole);
  const isResetLike = mode === 'reset' || mode === 'set';
  const title = isResetLike ? (isAdminRecipient ? 'Reset Password Akun Admin ESS' : 'Reset Password Akun ESS') : (isAdminRecipient ? 'Aktivasi Akun Admin ESS' : 'Aktivasi Akun ESS');
  const subject = isResetLike ? (isAdminRecipient ? 'Reset Password Akun Admin ESS - Trendhorizone Space' : 'Reset Password Akun ESS - Trendhorizone Space') : (isAdminRecipient ? 'Aktivasi Akun Admin ESS - Trendhorizone Space' : 'Aktivasi Akun ESS - Trendhorizone Space');
  const headline = isResetLike ? (isAdminRecipient ? 'Password akun admin ESS Anda telah diperbarui' : 'Password akun ESS Anda telah diperbarui') : (isAdminRecipient ? 'Akun admin ESS Anda siap digunakan' : 'Akun ESS Anda siap digunakan');
  const accessPath = isAdminRecipient ? '/admin' : '/employee';
  const accessLabel = isAdminRecipient ? 'Portal Admin' : 'Portal Karyawan';
  const accessUrl = appUrl + accessPath;
  const accessNote = isAdminRecipient ? 'Akun ini ditetapkan sebagai admin. Gunakan akses Admin Dashboard setelah login.' : 'Akun ini ditetapkan sebagai karyawan. Gunakan akses Dashboard Karyawan setelah login.';
  const html = '<div style="font-family:Inter,Arial,sans-serif;background:#f8fafc;padding:24px;color:#0f172a;">'
    + '<div style="max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;">'
    + '<div style="padding:18px 20px;background:linear-gradient(135deg,#1d4ed8,#4f46e5);color:#fff;">'
    + '<div style="font-size:18px;font-weight:800;letter-spacing:.2px;">Trendhorizone Space • Employee Self Service</div>'
    + '<div style="font-size:12px;opacity:.92;margin-top:4px;">' + title + '</div>'
    + '</div>'
    + '<div style="padding:20px;">'
    + '<p style="margin:0 0 10px;">Halo <b>' + String(employeeName || 'Karyawan') + '</b>,</p>'
    + '<p style="margin:0 0 16px;">' + headline + '. Silakan login menggunakan kredensial berikut:</p>'
    + '<table style="width:100%;border-collapse:collapse;margin-bottom:14px;">'
    + '<tr><td style="width:140px;padding:10px;border:1px solid #e2e8f0;background:#f8fafc;">Username</td><td style="padding:10px;border:1px solid #e2e8f0;"><b>' + username + '</b></td></tr>'
    + '<tr><td style="padding:10px;border:1px solid #e2e8f0;background:#f8fafc;">Password</td><td style="padding:10px;border:1px solid #e2e8f0;"><b>' + String(passwordPlain || '') + '</b></td></tr>'
    + '<tr><td style="padding:10px;border:1px solid #e2e8f0;background:#f8fafc;">Login URL</td><td style="padding:10px;border:1px solid #e2e8f0;"><a href="' + appUrl + '/login">' + appUrl + '/login</a></td></tr>'
    + '<tr><td style="padding:10px;border:1px solid #e2e8f0;background:#f8fafc;">Link Akses</td><td style="padding:10px;border:1px solid #e2e8f0;"><a href="' + accessUrl + '">' + accessLabel + '</a></td></tr>'
    + '</table>'
    + '<p style="margin:0 0 8px;">' + accessNote + '</p>'
    + '<p style="margin:0 0 8px;">Untuk keamanan akun, setelah login segera ubah password Anda.</p>'
    + '<p style="margin:0;color:#64748b;font-size:12px;">Email ini dikirim otomatis oleh sistem ESS ' + appUrl + '.</p>'
    + '</div>'
    + '</div>'
    + '</div>';
  const keySource = process.env.RESEND_API_KEY ? 'RESEND_API_KEY' : (process.env.RESEND_KEY ? 'RESEND_KEY' : (process.env.EMAIL_RESEND_API_KEY ? 'EMAIL_RESEND_API_KEY' : 'none'));
  if (!apiKey) return { sent: false, channel: 'manual', message: 'RESEND_API_KEY belum diset.', debug: { key_source: keySource, from: from, fallback_from: fallbackFrom, to: to } };
  async function sendWithFrom(fromAddress) {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + apiKey, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: fromAddress, to: [to], subject: subject, html: html })
    });
    const tx = await r.text();
    let data = null;
    try { data = JSON.parse(tx); } catch (_) { data = null; }
    return { ok: r.ok, status: r.status, data: data, raw: tx };
  }
  try {
    const senderCandidates = [from, autoFrom, fallbackFrom].filter(Boolean).filter(function(v, idx, arr) { return arr.findIndex(function(x) { return String(x).toLowerCase() === String(v).toLowerCase(); }) === idx; });
    const attempts = [];
    for (let i = 0; i < senderCandidates.length; i += 1) {
      const sender = senderCandidates[i];
      const rs = await sendWithFrom(sender);
      const detail = rs.data && (rs.data.message || rs.data.error || rs.data.name) ? String(rs.data.message || rs.data.error || rs.data.name) : String(rs.raw || '');
      attempts.push({ sender: sender, status: rs.status, detail: detail });
      if (rs.ok) return { sent: true, channel: 'resend', provider_id: rs.data && rs.data.id ? rs.data.id : '', sender: sender, warning: i > 0 ? 'Sender fallback dipakai.' : '', debug: { key_source: keySource, role: userRole, access_path: accessPath, from: from, auto_from: autoFrom, fallback_from: fallbackFrom, to: to, attempts: attempts } };
      const retryable = /domain|sender|from|verify|validation|unauthor/i.test(detail) || rs.status === 401 || rs.status === 403 || rs.status === 422;
      if (!retryable) break;
    }
    const compact = attempts.map(function(x) { return x.sender + ' => ' + x.detail; }).join(' | ');
    return { sent: false, channel: 'resend', message: 'Gagal kirim email.', error: compact, sender: from, fallback_sender: fallbackFrom, debug: { key_source: keySource, role: userRole, access_path: accessPath, from: from, auto_from: autoFrom, fallback_from: fallbackFrom, to: to, attempts: attempts } };
  } catch (e) {
    return { sent: false, channel: 'resend', message: 'Error kirim email aktivasi.', error: String(e && e.message || e || ''), debug: { key_source: keySource, role: userRole, access_path: accessPath, from: from, auto_from: autoFrom, fallback_from: fallbackFrom, to: to } };
  }
}
async function getAuthCredByEmail(email) {
  const e = String(email || '').trim().toLowerCase();
  if (!e) return null;
  const r = await db('GET', 'config', { select: 'key,value', key: 'eq.' + authCredKey(e), limit: 1 });
  if (!r.ok) return null;
  const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
  if (!row) return null;
  const val = safeJsonParse(row.value, {});
  return Object.assign({}, val, { key: row.key });
}
async function upsertAuthCred(email, payload) {
  const e = String(email || '').trim().toLowerCase();
  if (!e) return false;
  const key = authCredKey(e);
  const value = JSON.stringify(Object.assign({}, payload || {}, { email: e }));
  const up = await db('POST', 'config', { on_conflict: 'key' }, { key: key, value: value }, { Prefer: 'resolution=merge-duplicates,return=representation' });
  return !!up.ok;
}
async function createSessionForUser(user, ttlHours) {
  const token = crypto.randomBytes(24).toString('hex');
  const now = Date.now();
  const exp = new Date(now + Math.max(1, Number(ttlHours || 12)) * 3600 * 1000).toISOString();
  const key = authSessionKey(token);
  const value = JSON.stringify({
    token: token,
    employee_id: String(user.employee_id || ''),
    email: String(user.email || '').trim().toLowerCase(),
    role: String(user.role || 'employee').trim().toLowerCase(),
    expires_at: exp,
    created_at: nowIso()
  });
  const up = await db('POST', 'config', { on_conflict: 'key' }, { key: key, value: value }, { Prefer: 'resolution=merge-duplicates,return=minimal' });
  if (!up.ok) return null;
  return { token: token, expires_at: exp };
}
async function readSession(token) {
  const t = String(token || '').trim();
  if (!t) return null;
  const r = await db('GET', 'config', { select: 'key,value', key: 'eq.' + authSessionKey(t), limit: 1 });
  if (!r.ok) return null;
  const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
  if (!row) return null;
  const v = safeJsonParse(row.value, {});
  if (!v || String(v.token || '') !== t) return null;
  if (v.expires_at && new Date(v.expires_at).getTime() < Date.now()) {
    await db('DELETE', 'config', { key: 'eq.' + authSessionKey(t) });
    return null;
  }
  return v;
}
async function deleteSession(token) {
  const t = String(token || '').trim();
  if (!t) return false;
  const r = await db('DELETE', 'config', { key: 'eq.' + authSessionKey(t) });
  return !!r.ok;
}
function bearerToken(req) {
  const h = String(req.headers.authorization || req.headers.Authorization || '').trim();
  if (!/^Bearer\s+/i.test(h)) return '';
  return h.replace(/^Bearer\s+/i, '').trim();
}
function toMoney(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 100) / 100;
}
const PAYROLL_SUMMARY_CACHE = new Map();
function cacheGet(key) {
  const v = PAYROLL_SUMMARY_CACHE.get(key);
  if (!v) return null;
  if (Date.now() > Number(v.expired_at || 0)) { PAYROLL_SUMMARY_CACHE.delete(key); return null; }
  return v.data;
}
function cacheSet(key, data, ttlMs) {
  PAYROLL_SUMMARY_CACHE.set(key, { data: data, expired_at: Date.now() + Math.max(1000, Number(ttlMs || 300000)) });
}
const PAYROLL_COMPONENTS = [
  { name: 'Basic Salary', type: 'EARNING', category: 'FIXED' },
  { name: 'Allowance', type: 'EARNING', category: 'FIXED' },
  { name: 'Transport Allowance', type: 'EARNING', category: 'FIXED' },
  { name: 'Meal Allowance', type: 'EARNING', category: 'FIXED' },
  { name: 'Overtime Pay', type: 'EARNING', category: 'VARIABLE' },
  { name: 'Bonus / Incentive', type: 'EARNING', category: 'VARIABLE' },
  { name: 'Attendance Allowance', type: 'EARNING', category: 'VARIABLE' },
  { name: 'BPJS KESEHATAN PERUSAHAAN 4% ALLOWANCE', type: 'EARNING', category: 'VARIABLE' },
  { name: 'JKK 0.24% ALLOWANCE', type: 'EARNING', category: 'VARIABLE' },
  { name: 'JKM 0.3% ALLOWANCE', type: 'EARNING', category: 'VARIABLE' },
  { name: 'JAMINAN PENSIUN -2% PERUSAHAAN ADDITION', type: 'EARNING', category: 'VARIABLE' },
  { name: 'JHT -3.7% BY COMPANY ADDITION', type: 'EARNING', category: 'VARIABLE' },
  { name: 'BPJS / Insurance', type: 'DEDUCTION', category: 'FIXED' },
  { name: 'Tax (PPh21)', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'Penalty / Deduction', type: 'DEDUCTION', category: 'FIXED' },
  { name: 'Late Deduction', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'Absence Deduction', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'Other Deduction', type: 'DEDUCTION', category: 'FIXED' },
  { name: 'BPJS KESEHATAN KARYAWAN 1% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'BPJS KESEHATAN PERUSAHAAN 4% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'JAMINAN PENSIUN 1% KARYAWAN DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'JAMINAN PENSIUN 2% PERUSAHAAN DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'JHT 2% BY EMPLOYEE DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'JHT 3.7% BY COMPANY DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'JKK 0.24% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'JKM 0.3% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE' },
  { name: 'THR', type: 'EARNING', category: 'VARIABLE' }
];
function payrollComponentKey(name) {
  return String(name || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}
function defaultFormulaByName(name) {
  const n = String(name || '').toLowerCase().trim();
  if (n === 'basic salary') return '(worked_days / total_work_days) * full_salary';
  if (n === 'overtime pay') return 'overtime_hours * rate_per_hour';
  if (n === 'attendance allowance') return '(present_days / total_days) * base_allowance';
  if (n === 'tax (pph21)') return 'gross_salary * tax_rate';
  if (n === 'late deduction') return 'late_minutes * penalty_rate';
  if (n === 'absence deduction') return 'absent_days * daily_salary';
  if (n === 'bpjs kesehatan karyawan 1% deduction') return 'MaxMinFormula(4500000, UMP_JAM, MAX_KS, 0.01)';
  if (n === 'bpjs kesehatan perusahaan 4% allowance') return 'MaxMinFormula(4500000, UMP_JAM, MAX_KS, 0.04)';
  if (n === 'bpjs kesehatan perusahaan 4% deduction') return 'MaxMinFormula(4500000, UMP_JAM, MAX_KS, 0.04)';
  if (n === 'jaminan pensiun 1% karyawan deduction') return 'MaximumFormula(UMP_JAM, MAX_JP, 0.01)';
  if (n === 'jaminan pensiun 2% perusahaan deduction') return 'MaximumFormula(UMP_JAM, MAX_JP, 0.02)';
  if (n === 'jaminan pensiun -2% perusahaan addition') return 'MaximumFormula(UMP_JAM, MAX_JP, -0.02)';
  if (n === 'jht 2% by employee deduction') return 'UMP_JAM * 0.02';
  if (n === 'jht -3.7% by company addition') return 'UMP_JAM * -0.037';
  if (n === 'jht 3.7% by company deduction') return 'UMP_JAM * 0.037';
  if (n === 'jkk 0.24% allowance') return 'UMP_JAM * 0.0024';
  if (n === 'jkk 0.24% deduction') return 'UMP_JAM * 0.0024';
  if (n === 'jkm 0.3% allowance') return 'UMP_JAM * 0.003';
  if (n === 'jkm 0.3% deduction') return 'UMP_JAM * 0.003';
  return '';
}
function shouldApplyDefaultFormula(name, ctx) {
  const n = String(name || '').toLowerCase().trim();
  if (n === 'basic salary') return Number(ctx.worked_days || 0) > 0 && Number(ctx.full_salary || 0) > 0;
  if (n === 'overtime pay') return Number(ctx.overtime_hours || 0) > 0 && Number(ctx.rate_per_hour || 0) > 0;
  if (n === 'attendance allowance') return Number(ctx.base_allowance || 0) > 0 && Number(ctx.present_days || 0) > 0;
  if (n === 'tax (pph21)') return Number(ctx.tax_rate || 0) > 0;
  if (n === 'late deduction') return Number(ctx.late_minutes || 0) > 0 && Number(ctx.penalty_rate || 0) > 0;
  if (n === 'absence deduction') return Number(ctx.absent_days || 0) > 0 && Number(ctx.daily_salary || 0) > 0;
  if (n.indexOf('bpjs kesehatan') >= 0) return Number(ctx.UMP_JAM || 0) > 0;
  if (n.indexOf('jaminan pensiun') >= 0) return Number(ctx.UMP_JAM || 0) > 0;
  if (n.indexOf('jht') >= 0 || n.indexOf('jkk') >= 0 || n.indexOf('jkm') >= 0) return Number(ctx.UMP_JAM || 0) > 0;
  return false;
}
function formulaEvaluator(formula, scope) {
  const f = String(formula || '').trim();
  if (!f) return null;
  if (!/^[a-zA-Z0-9_+\-*/().<>=!?:,\s]+$/.test(f)) return null;
  const keys = Object.keys(scope || {});
  const values = keys.map(function(k) { return scope[k]; });
  try {
    const fn = new Function(...keys, 'return (' + f + ')');
    const out = fn(...values);
    return toMoney(out);
  } catch (_) {
    return null;
  }
}
function componentMapper(input) {
  const src = input || {};
  const given = Array.isArray(src.components) ? src.components : [];
  const mapped = [];
  given.forEach(function(c) {
    const base = PAYROLL_COMPONENTS.find(function(x) { return String(x.name).toLowerCase() === String(c.name || '').toLowerCase(); }) || null;
    mapped.push({
      name: String(c.name || (base ? base.name : '')).trim(),
      type: String(c.type || (base ? base.type : 'EARNING')).toUpperCase() === 'DEDUCTION' ? 'DEDUCTION' : 'EARNING',
      category: String(c.category || (base ? base.category : 'VARIABLE')).toUpperCase() === 'FIXED' ? 'FIXED' : 'VARIABLE',
      value: toMoney(c.value),
      formula: String(c.formula || '').trim()
    });
  });
  if (!mapped.length) {
    const val = function(v) { return toMoney(v); };
    mapped.push({ name: 'Basic Salary', type: 'EARNING', category: 'FIXED', value: val(src.gaji_pokok || src.basic_salary || src.full_salary), formula: String(src.basic_salary_formula || '').trim() });
    mapped.push({ name: 'Allowance', type: 'EARNING', category: 'FIXED', value: val(src.tunjangan || src.allowance), formula: '' });
    mapped.push({ name: 'Transport Allowance', type: 'EARNING', category: 'FIXED', value: val(src.transport_allowance), formula: '' });
    mapped.push({ name: 'Meal Allowance', type: 'EARNING', category: 'FIXED', value: val(src.meal_allowance), formula: '' });
    mapped.push({ name: 'Overtime Pay', type: 'EARNING', category: 'VARIABLE', value: val(src.lembur || src.overtime_pay), formula: String(src.overtime_formula || '').trim() });
    mapped.push({ name: 'Bonus / Incentive', type: 'EARNING', category: 'VARIABLE', value: val(src.bonus), formula: '' });
    mapped.push({ name: 'Attendance Allowance', type: 'EARNING', category: 'VARIABLE', value: val(src.attendance_allowance), formula: String(src.attendance_formula || '').trim() });
    mapped.push({ name: 'THR', type: 'EARNING', category: 'VARIABLE', value: val(src.thr), formula: '' });
    mapped.push({ name: 'BPJS KESEHATAN PERUSAHAAN 4% ALLOWANCE', type: 'EARNING', category: 'VARIABLE', value: val(src.bpjs_kesehatan_perusahaan_allowance), formula: '' });
    mapped.push({ name: 'JKK 0.24% ALLOWANCE', type: 'EARNING', category: 'VARIABLE', value: val(src.jkk_allowance), formula: '' });
    mapped.push({ name: 'JKM 0.3% ALLOWANCE', type: 'EARNING', category: 'VARIABLE', value: val(src.jkm_allowance), formula: '' });
    mapped.push({ name: 'JAMINAN PENSIUN -2% PERUSAHAAN ADDITION', type: 'EARNING', category: 'VARIABLE', value: val(src.jaminan_pensiun_perusahaan_addition), formula: '' });
    mapped.push({ name: 'JHT -3.7% BY COMPANY ADDITION', type: 'EARNING', category: 'VARIABLE', value: val(src.jht_company_addition), formula: '' });
    mapped.push({ name: 'Tax (PPh21)', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.potongan_pajak || src.tax), formula: String(src.tax_formula || '').trim() });
    mapped.push({ name: 'BPJS / Insurance', type: 'DEDUCTION', category: 'FIXED', value: val((src.bpjs_kesehatan || 0) + (src.bpjs_ketenagakerjaan || 0) + (src.bpjs || 0)), formula: '' });
    mapped.push({ name: 'Penalty / Deduction', type: 'DEDUCTION', category: 'FIXED', value: val(src.penalty_deduction), formula: '' });
    mapped.push({ name: 'Late Deduction', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.late_deduction), formula: String(src.late_formula || '').trim() });
    mapped.push({ name: 'Absence Deduction', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.absence_deduction), formula: String(src.absence_formula || '').trim() });
    mapped.push({ name: 'Other Deduction', type: 'DEDUCTION', category: 'FIXED', value: val(src.potongan_lain || src.other_deduction), formula: '' });
    mapped.push({ name: 'BPJS KESEHATAN KARYAWAN 1% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.bpjs_kesehatan_karyawan_deduction || src.bpjs_kesehatan), formula: '' });
    mapped.push({ name: 'BPJS KESEHATAN PERUSAHAAN 4% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.bpjs_kesehatan_perusahaan_deduction), formula: '' });
    mapped.push({ name: 'JAMINAN PENSIUN 1% KARYAWAN DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.jaminan_pensiun_karyawan_deduction), formula: '' });
    mapped.push({ name: 'JAMINAN PENSIUN 2% PERUSAHAAN DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.jaminan_pensiun_perusahaan_deduction), formula: '' });
    mapped.push({ name: 'JHT 2% BY EMPLOYEE DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.jht_employee_deduction || src.bpjs_ketenagakerjaan), formula: '' });
    mapped.push({ name: 'JHT 3.7% BY COMPANY DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.jht_company_deduction), formula: '' });
    mapped.push({ name: 'JKK 0.24% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.jkk_deduction), formula: '' });
    mapped.push({ name: 'JKM 0.3% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: val(src.jkm_deduction), formula: '' });
  }
  const byName = {};
  mapped.forEach(function(c) { byName[String(c.name || '').toLowerCase()] = c; });
  PAYROLL_COMPONENTS.forEach(function(t) {
    if (!byName[String(t.name).toLowerCase()]) mapped.push({ name: t.name, type: t.type, category: t.category, value: 0, formula: '' });
  });
  return mapped.map(function(c) {
    return {
      name: String(c.name || '').trim(),
      type: String(c.type || 'EARNING').toUpperCase() === 'DEDUCTION' ? 'DEDUCTION' : 'EARNING',
      category: String(c.category || 'VARIABLE').toUpperCase() === 'FIXED' ? 'FIXED' : 'VARIABLE',
      value: toMoney(c.value),
      formula: String(c.formula || '').trim()
    };
  });
}
function maxMinFormula(calculationBase, minimumCalculationBase, maximumCalculationBase, tariff) {
  const minBase = Number(minimumCalculationBase || 0);
  const maxBase = Number(maximumCalculationBase || 0);
  let base = Number(calculationBase || 0);
  if (Number.isFinite(minBase) && minBase > 0) base = Math.max(base, minBase);
  if (Number.isFinite(maxBase) && maxBase > 0) base = Math.min(base, maxBase);
  return toMoney(base * Number(tariff || 0));
}
function maximumFormula(calculationBase, maximumCalculationBase, tariff) {
  const base = Number(calculationBase || 0);
  const maxBase = Number(maximumCalculationBase || 0);
  const used = Number.isFinite(maxBase) && maxBase > 0 ? Math.min(base, maxBase) : base;
  return toMoney(used * Number(tariff || 0));
}
function componentEligible(name, ctx) {
  const n = String(name || '').toLowerCase();
  const applyKes = String(ctx.apply_bpjs_kesehatan).toLowerCase() !== 'false';
  const applyKetenaga = String(ctx.apply_bpjs_ketenagakerjaan).toLowerCase() !== 'false';
  const applyTax = String(ctx.apply_tax).toLowerCase() !== 'false';
  if (n.indexOf('tax') >= 0 || n.indexOf('pph21') >= 0) return applyTax;
  if (n.indexOf('bpjs kesehatan') >= 0) return applyKes;
  if (n.indexOf('jht') >= 0 || n.indexOf('jkk') >= 0 || n.indexOf('jkm') >= 0 || n.indexOf('jaminan pensiun') >= 0) return applyKetenaga;
  return true;
}
function payrollEngine(input) {
  const employeeId = String((input && input.employee_id) || '').trim();
  const components = componentMapper(input);
  const ctx = Object.assign({
    worked_days: 0, total_work_days: 1, full_salary: 0, overtime_hours: 0, rate_per_hour: 0, present_days: 0, total_days: 1, base_allowance: 0, tax_rate: 0, late_minutes: 0, penalty_rate: 0, absent_days: 0, daily_salary: 0, gross_salary: 0
  }, (input && input.context) || {});
  ctx.worked_days = Number(ctx.worked_days || 0);
  ctx.total_work_days = Math.max(1, Number(ctx.total_work_days || 1));
  ctx.overtime_hours = Number(ctx.overtime_hours || 0);
  ctx.rate_per_hour = Number(ctx.rate_per_hour || 0);
  ctx.present_days = Number(ctx.present_days || 0);
  ctx.total_days = Math.max(1, Number(ctx.total_days || 1));
  ctx.base_allowance = Number(ctx.base_allowance || 0);
  ctx.tax_rate = Number(ctx.tax_rate || 0);
  ctx.late_minutes = Number(ctx.late_minutes || 0);
  ctx.penalty_rate = Number(ctx.penalty_rate || 0);
  ctx.absent_days = Number(ctx.absent_days || 0);
  ctx.UMP_JAM = Number(ctx.UMP_JAM || ctx.ump_jam || ctx.full_salary || 0);
  ctx.MAX_KS = Number(ctx.MAX_KS || ctx.max_ks || 12000000);
  ctx.MAX_JP = Number(ctx.MAX_JP || ctx.max_jp || 10300000);
  if (ctx.apply_bpjs_kesehatan === undefined) ctx.apply_bpjs_kesehatan = true;
  if (ctx.apply_bpjs_ketenagakerjaan === undefined) ctx.apply_bpjs_ketenagakerjaan = true;
  if (ctx.apply_tax === undefined) ctx.apply_tax = true;
  ctx.MaxMinFormula = maxMinFormula;
  ctx.MaximumFormula = maximumFormula;
  const basicComp = components.find(function(c) { return String(c.name).toLowerCase() === 'basic salary'; }) || { value: 0 };
  ctx.full_salary = Number(ctx.full_salary || basicComp.value || 0);
  if (!ctx.UMP_JAM || ctx.UMP_JAM <= 0) ctx.UMP_JAM = Number(ctx.full_salary || 0);
  ctx.daily_salary = Number(ctx.daily_salary || (ctx.total_work_days > 0 ? ctx.full_salary / ctx.total_work_days : 0));
  const evaluateComponent = function(comp) {
    if (!componentEligible(comp.name, ctx)) {
      ctx[payrollComponentKey(comp.name)] = 0;
      return Object.assign({}, comp, { value: 0, formula: String(comp.formula || '') });
    }
    const manualFormula = String(comp.formula || '').trim();
    const autoFormula = manualFormula ? '' : (shouldApplyDefaultFormula(comp.name, ctx) ? defaultFormulaByName(comp.name) : '');
    const formula = String(manualFormula || autoFormula || '').trim();
    let value = toMoney(comp.value);
    if (formula) {
      const evalVal = formulaEvaluator(formula, ctx);
      if (evalVal !== null && Number.isFinite(evalVal)) value = toMoney(evalVal);
    }
    if (!Number.isFinite(value)) value = 0;
    ctx[payrollComponentKey(comp.name)] = value;
    return Object.assign({}, comp, { value: toMoney(value), formula: formula || '' });
  };
  const fixedEarnings = components.filter(function(c) { return c.type === 'EARNING' && c.category === 'FIXED'; }).map(evaluateComponent);
  const variableEarnings = components.filter(function(c) { return c.type === 'EARNING' && c.category !== 'FIXED'; }).map(evaluateComponent);
  const allEarnings = fixedEarnings.concat(variableEarnings);
  const totalEarning = toMoney(allEarnings.reduce(function(acc, x) { return acc + Number(x.value || 0); }, 0));
  const grossSalary = totalEarning;
  ctx.gross_salary = grossSalary;
  const variableDeductions = components.filter(function(c) { return c.type === 'DEDUCTION' && c.category !== 'FIXED'; }).map(evaluateComponent);
  const fixedDeductions = components.filter(function(c) { return c.type === 'DEDUCTION' && c.category === 'FIXED'; }).map(evaluateComponent);
  const allDeductions = variableDeductions.concat(fixedDeductions);
  const totalDeduction = toMoney(allDeductions.reduce(function(acc, x) { return acc + Number(x.value || 0); }, 0));
  const netSalary = toMoney(grossSalary - totalDeduction);
  const errors = [];
  const warnings = [];
  const basicVal = toMoney((allEarnings.find(function(x) { return String(x.name).toLowerCase() === 'basic salary'; }) || {}).value || 0);
  if (basicVal === 0) errors.push('Basic Salary bernilai 0.');
  if (totalDeduction > grossSalary) warnings.push('Total deduction lebih besar dari gross salary.');
  return {
    employee_id: employeeId,
    total_earning: totalEarning,
    total_deduction: totalDeduction,
    gross_salary: grossSalary,
    net_salary: netSalary,
    breakdown: { earnings: allEarnings, deductions: allDeductions },
    errors: errors,
    warnings: warnings
  };
}
function parsePayrollMetaFromKeterangan(text) {
  const raw = String(text || '');
  const marker = 'PAYROLL_META::';
  const idx = raw.indexOf(marker);
  if (idx < 0) return null;
  const jsonText = raw.slice(idx + marker.length).trim();
  try {
    const m = JSON.parse(jsonText);
    if (m && m.payroll_output) return m;
    if (m && Array.isArray(m.components)) return { version: 2, payroll_output: payrollEngine({ employee_id: m.employee_id || '', components: m.components, context: m.context || {} }), components: m.components, context: m.context || {} };
    return { version: 1, payroll_output: payrollEngine(m), legacy: true };
  } catch (_) {
    return null;
  }
}
function composePayrollKeterangan(note, data) {
  const clean = String(note || '').replace(/\n*\s*PAYROLL_META::[\s\S]*$/m, '').trim();
  const encoded = 'PAYROLL_META::' + JSON.stringify(data || {});
  return clean ? (clean + '\n' + encoded) : encoded;
}
function enrichPayrollDoc(row) {
  const r = Object.assign({}, row || {});
  const meta = parsePayrollMetaFromKeterangan(r.keterangan || '');
  if (!meta || !meta.payroll_output) {
    const empty = payrollEngine({ employee_id: String(r.employee_id || ''), components: [] });
    Object.assign(r, empty);
    return r;
  }
  Object.assign(r, meta.payroll_output);
  r.components = meta.components || [];
  r.context = meta.context || {};
  return r;
}
function componentMapperFromMatrixRows(rows) {
  const grouped = {};
  (rows || []).forEach(function(row) {
    const employeeId = String(row.employee_id || '').trim();
    if (!employeeId) return;
    if (!grouped[employeeId]) grouped[employeeId] = { employee_id: employeeId, components: [] };
    const componentName = String(row.component_name || row.name || row.remark || '').trim();
    if (!componentName) return;
    grouped[employeeId].components.push({
      name: componentName,
      type: String(row.type || '').toUpperCase() === 'DEDUCTION' ? 'DEDUCTION' : 'EARNING',
      category: String(row.category || '').toUpperCase() === 'FIXED' ? 'FIXED' : 'VARIABLE',
      value: toMoney(row.value),
      formula: String(row.formula || '').trim()
    });
  });
  return Object.values(grouped);
}
async function validateDivisionAndPosition(divisi, jabatan) {
  const divName = String(divisi || '').trim();
  const posName = String(jabatan || '').trim();
  if (!divName || !posName) return { ok: false, message: 'divisi dan jabatan wajib diisi.' };
  const div = await db('GET', 'divisions', { select: 'division_id,nama_divisi,is_active', nama_divisi: 'eq.' + divName, limit: 1 });
  if (!div.ok) return { ok: false, message: 'Gagal validasi divisi.', error: div.error };
  const divRow = Array.isArray(div.data) && div.data[0] ? div.data[0] : null;
  if (!divRow || !(divRow.is_active === true || String(divRow.is_active).toLowerCase() === 'true')) return { ok: false, message: 'Divisi tidak ditemukan atau tidak aktif.' };
  const pos = await db('GET', 'positions', { select: 'position_id,nama_jabatan,division_id,is_active', nama_jabatan: 'eq.' + posName, limit: 1 });
  if (!pos.ok) return { ok: false, message: 'Gagal validasi jabatan.', error: pos.error };
  const posRow = Array.isArray(pos.data) && pos.data[0] ? pos.data[0] : null;
  if (!posRow || !(posRow.is_active === true || String(posRow.is_active).toLowerCase() === 'true')) return { ok: false, message: 'Jabatan tidak ditemukan atau tidak aktif.' };
  if (String(posRow.division_id || '').trim() && String(posRow.division_id || '').trim() !== String(divRow.division_id || '').trim()) return { ok: false, message: 'Jabatan tidak sesuai dengan divisi terpilih.' };
  return { ok: true };
}
function parseCsvTextRows(csvText) {
  const src = String(csvText || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const rows = [];
  let row = [];
  let cell = '';
  let inQuotes = false;
  for (let i = 0; i < src.length; i += 1) {
    const ch = src[i];
    if (inQuotes) {
      if (ch === '"') {
        if (src[i + 1] === '"') {
          cell += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cell += ch;
      }
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
      continue;
    }
    if (ch === ',') {
      row.push(cell);
      cell = '';
      continue;
    }
    if (ch === '\n') {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = '';
      continue;
    }
    cell += ch;
  }
  row.push(cell);
  rows.push(row);
  while (rows.length && rows[rows.length - 1].every(function(c) { return String(c || '').trim() === ''; })) rows.pop();
  return rows;
}
function csvRowsToObjects(csvText) {
  const matrix = parseCsvTextRows(csvText);
  if (!matrix.length) return [];
  const headers = (matrix[0] || []).map(function(h) { return String(h || '').replace(/^\uFEFF/, '').trim(); });
  const out = [];
  for (let i = 1; i < matrix.length; i += 1) {
    const row = matrix[i] || [];
    const obj = {};
    let hasValue = false;
    headers.forEach(function(key, idx) {
      if (!key) return;
      const val = row[idx] === undefined ? '' : row[idx];
      if (String(val || '').trim() !== '') hasValue = true;
      obj[key] = val;
    });
    if (hasValue) out.push(obj);
  }
  return out;
}
function toBoolDefaultTrue(v) {
  if (v === undefined || v === null || String(v).trim() === '') return true;
  return String(v).toLowerCase() === 'true' || v === true || String(v) === '1';
}
function employeePayloadFromInput(input) {
  const b = input || {};
  return {
    employee_id: String(b.employee_id || rid('EMP')).trim(),
    email: String(b.email || '').trim().toLowerCase(),
    nama: String(b.nama || '').trim(),
    nik: String(b.nik || '').trim(),
    divisi: String(b.divisi || '').trim(),
    jabatan: String(b.jabatan || '').trim(),
    atasan_email: String(b.atasan_email || '').trim().toLowerCase(),
    status_karyawan: String(b.status_karyawan || 'Tetap').trim(),
    tanggal_masuk: b.tanggal_masuk || null,
    jatah_cuti: Number(b.jatah_cuti || 12),
    sisa_cuti: Number(b.sisa_cuti || b.jatah_cuti || 12),
    role: String(b.role || 'employee').trim().toLowerCase(),
    is_active: toBoolDefaultTrue(b.is_active),
    no_hp: String(b.no_hp || '').trim(),
    alamat: String(b.alamat || '').trim(),
    created_at: nowIso(),
    updated_at: nowIso()
  };
}
function employeePayloadValidationMessage(payload) {
  const allowedRoles = ['employee', 'admin', 'superadmin', 'manager'];
  const allowedStatus = ['Tetap', 'Kontrak', 'Magang', 'Probation', 'Outsource'];
  if (!payload.email || !payload.nama) return 'email dan nama wajib diisi.';
  if (!isValidEmail(payload.email)) return 'Format email tidak valid.';
  if (!allowedRoles.includes(payload.role)) return 'Role tidak valid.';
  if (!allowedStatus.includes(payload.status_karyawan)) return 'Status karyawan tidak valid.';
  if (!payload.tanggal_masuk || !isValidDateYmd(payload.tanggal_masuk)) return 'tanggal_masuk wajib format YYYY-MM-DD.';
  if (Number(payload.jatah_cuti) < 0 || Number(payload.sisa_cuti) < 0) return 'Jatah/sisa cuti tidak boleh negatif.';
  if (Number(payload.sisa_cuti) > Number(payload.jatah_cuti)) return 'Sisa cuti tidak boleh lebih besar dari jatah cuti.';
  return '';
}
function normalizeApiErrorMessage(err, fallback) {
  if (!err) return String(fallback || 'Request failed');
  if (typeof err === 'string') return err;
  if (Array.isArray(err) && err[0] && err[0].message) return String(err[0].message);
  if (err.message) return String(err.message);
  return String(fallback || 'Request failed');
}
function normalizeNameKey(v) {
  return String(v || '').trim().toLowerCase().replace(/\s+/g, ' ');
}
async function syncDivisionPositionFromEmployees(actorEmail) {
  const em = await db('GET', 'employees', { select: 'employee_id,divisi,jabatan,is_active', limit: 5000 });
  if (!em.ok) return { ok: false, message: 'Gagal ambil data employee.', error: em.error };
  const divRows = await db('GET', 'divisions', { select: 'division_id,nama_divisi,is_active', limit: 5000 });
  if (!divRows.ok) return { ok: false, message: 'Gagal ambil data divisi.', error: divRows.error };
  const posRows = await db('GET', 'positions', { select: 'position_id,nama_jabatan,division_id,is_active', limit: 5000 });
  if (!posRows.ok) return { ok: false, message: 'Gagal ambil data jabatan.', error: posRows.error };

  const existingDivs = Array.isArray(divRows.data) ? divRows.data : [];
  const existingPos = Array.isArray(posRows.data) ? posRows.data : [];
  const divByName = {};
  existingDivs.forEach(function(d) { divByName[normalizeNameKey(d.nama_divisi)] = d; });

  let createdDivisions = 0;
  let createdPositions = 0;

  for (const emp of (Array.isArray(em.data) ? em.data : [])) {
    const divName = String(emp.divisi || '').trim();
    const posName = String(emp.jabatan || '').trim();
    if (!divName) continue;
    const divKey = normalizeNameKey(divName);
    let div = divByName[divKey];
    if (!div) {
      const payload = { division_id: rid('DIV'), nama_divisi: divName, kepala_divisi_email: null, is_active: true, updated_at: nowIso() };
      const ins = await db('POST', 'divisions', null, payload, { Prefer: 'return=representation' });
      if (ins.ok && Array.isArray(ins.data) && ins.data[0]) {
        div = ins.data[0];
        divByName[divKey] = div;
        createdDivisions += 1;
      }
    } else if (String(div.is_active).toLowerCase() !== 'true') {
      await db('PATCH', 'divisions', { division_id: 'eq.' + String(div.division_id || '') }, { is_active: true, updated_at: nowIso() }, { Prefer: 'return=minimal' });
    }
    if (!posName || !div || !div.division_id) continue;
    const posKey = normalizeNameKey(posName) + '|' + String(div.division_id);
    const foundPos = existingPos.find(function(p) { return (normalizeNameKey(p.nama_jabatan) + '|' + String(p.division_id || '')) === posKey; });
    if (foundPos) {
      if (String(foundPos.is_active).toLowerCase() !== 'true') {
        await db('PATCH', 'positions', { position_id: 'eq.' + String(foundPos.position_id || '') }, { is_active: true, updated_at: nowIso() }, { Prefer: 'return=minimal' });
      }
      continue;
    }
    const insPos = await db('POST', 'positions', null, { position_id: rid('POS'), nama_jabatan: posName, division_id: String(div.division_id || ''), is_active: true, updated_at: nowIso() }, { Prefer: 'return=representation' });
    if (insPos.ok && Array.isArray(insPos.data) && insPos.data[0]) {
      existingPos.push(insPos.data[0]);
      createdPositions += 1;
    }
  }
  if (actorEmail) {
    await auditLog(actorEmail, 'SYNC', 'master_data', 'Sinkronisasi divisi/jabatan dari employee. divisi+' + createdDivisions + ', jabatan+' + createdPositions, '');
  }
  return { ok: true, created_divisions: createdDivisions, created_positions: createdPositions, total_employees: Array.isArray(em.data) ? em.data.length : 0 };
}
function protectedEmployeeIds() {
  const fromEnv = String(process.env.PROTECTED_EMPLOYEE_IDS || '').split(',').map(function(x) { return String(x || '').trim(); }).filter(Boolean);
  const defaults = ['EMP_ADMIN_001'];
  return Array.from(new Set(defaults.concat(fromEnv)));
}
function protectedEmployeeEmails() {
  const fromEnv = String(process.env.PROTECTED_EMPLOYEE_EMAILS || '').split(',').map(function(x) { return String(x || '').trim().toLowerCase(); }).filter(Boolean);
  const defaults = ['admin@company.com'];
  return Array.from(new Set(defaults.concat(fromEnv)));
}
function authResetRateKey(employeeId) {
  return 'AUTH_RESET_RATE_' + String(employeeId || '').trim();
}
async function consumeResetRateLimit(employeeId, actorEmail) {
  const id = String(employeeId || '').trim();
  if (!id) return { ok: false, message: 'employee_id wajib diisi.' };
  const key = authResetRateKey(id);
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const maxCount = 3;
  const cur = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
  let rec = { window_start: now, count: 0, updated_by: String(actorEmail || ''), updated_at: nowIso() };
  if (cur.ok && Array.isArray(cur.data) && cur.data[0] && cur.data[0].value) {
    const old = safeJsonParse(cur.data[0].value, {});
    const ws = Number(old.window_start || 0);
    const cnt = Number(old.count || 0);
    if (ws > 0 && now - ws < windowMs) rec = { window_start: ws, count: cnt, updated_by: String(old.updated_by || ''), updated_at: String(old.updated_at || '') };
  }
  if (rec.count >= maxCount && now - rec.window_start < windowMs) {
    return { ok: false, message: 'Reset password terlalu sering. Coba lagi beberapa menit.', retry_seconds: Math.max(1, Math.ceil((windowMs - (now - rec.window_start)) / 1000)) };
  }
  rec.count = (now - rec.window_start >= windowMs) ? 1 : (rec.count + 1);
  if (now - rec.window_start >= windowMs) rec.window_start = now;
  rec.updated_by = String(actorEmail || '');
  rec.updated_at = nowIso();
  await db('POST', 'config', { on_conflict: 'key' }, { key: key, value: JSON.stringify(rec) }, { Prefer: 'resolution=merge-duplicates,return=minimal' });
  return { ok: true, remaining: Math.max(0, maxCount - rec.count) };
}
async function recordActivationDelivery(employeeId, email, passwordPlain, delivery, mode, actorEmail) {
  const id = String(employeeId || '').trim();
  const mail = String(email || '').trim().toLowerCase();
  const d = delivery || {};
  const payload = {
    to: mail,
    activation_password: String(passwordPlain || ''),
    created_at: nowIso(),
    mode: String(mode || 'activation'),
    sent_via: d.channel || 'manual',
    sent: !!d.sent,
    sender: String(d.sender || ''),
    provider_id: String(d.provider_id || ''),
    warning: String(d.warning || ''),
    error: d.sent ? '' : String(d.error || d.message || '')
  };
  await db('POST', 'config', { on_conflict: 'key' }, { key: 'AUTH_ACTIVATION_OUTBOX_' + id, value: JSON.stringify(payload) }, { Prefer: 'resolution=merge-duplicates,return=minimal' });
  await db('POST', 'config', null, { key: 'AUTH_EMAIL_AUDIT_' + id + '_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6), value: JSON.stringify(Object.assign({}, payload, { actor: String(actorEmail || ''), debug: d.debug || null })) }, { Prefer: 'return=minimal' });
}
async function createEmployeeWithActivation(payload, options) {
  const opts = options || {};
  const val = employeePayloadValidationMessage(payload);
  if (val) return { ok: false, status: 400, message: val };
  if (!opts.skipDivisionValidation) {
    const vdp = await validateDivisionAndPosition(payload.divisi, payload.jabatan);
    if (!vdp.ok) return { ok: false, status: 400, message: vdp.message, error: vdp.error };
  }
  const ins = await db('POST', 'employees', null, payload, { Prefer: 'return=representation' });
  if (!ins.ok) return { ok: false, status: 500, message: 'Gagal tambah employee.', error: ins.error };
  const activationPassword = randomPassword(10);
  await upsertAuthCred(payload.email, {
    employee_id: payload.employee_id,
    password_hash: hashSha256(activationPassword),
    must_change_password: true,
    first_login_required: true,
    activation_sent_at: nowIso(),
    password_last_set_at: nowIso()
  });
  const delivery = await deliverActivationEmail(payload.email, payload.nama, activationPassword, { username: payload.email, mode: 'activation', role: payload.role });
  await recordActivationDelivery(payload.employee_id, payload.email, activationPassword, delivery, 'activation', String(opts.actor_email || ''));
  return {
    ok: true,
    message: delivery.sent ? 'Employee berhasil ditambahkan. Password aktivasi terkirim ke email.' : 'Employee berhasil ditambahkan. Password aktivasi dibuat, kirim manual jika email belum aktif.',
    activation_password: activationPassword,
    activation_delivery: delivery,
    data: ins.data
  };
}
async function deleteEmployeeCompletely(employeeId, actorEmail, ip, actorEmployeeId) {
  const id = String(employeeId || '').trim();
  if (!id) return { ok: false, status: 400, message: 'employee_id wajib diisi.' };
  const cur = await db('GET', 'employees', { select: 'employee_id,email,nama', employee_id: 'eq.' + id, limit: 1 });
  if (!cur.ok) return { ok: false, status: 500, message: 'Gagal validasi employee.', error: cur.error };
  const row = Array.isArray(cur.data) && cur.data[0] ? cur.data[0] : null;
  if (!row) return { ok: false, status: 404, message: 'Employee tidak ditemukan.' };
  const email = String(row.email || '').trim().toLowerCase();
  const actorMail = String(actorEmail || '').trim().toLowerCase();
  const actorEmp = String(actorEmployeeId || '').trim();
  if ((actorEmp && actorEmp === id) || (actorMail && actorMail === email)) return { ok: false, status: 403, message: 'Tidak dapat menghapus akun yang sedang digunakan login.' };
  if (protectedEmployeeIds().includes(id) || protectedEmployeeEmails().includes(email)) return { ok: false, status: 403, message: 'Akun protected tidak dapat dihapus.' };
  await db('DELETE', 'attendance', { employee_id: 'eq.' + id });
  await db('DELETE', 'leave_requests', { employee_id: 'eq.' + id });
  await db('DELETE', 'payroll_docs', { employee_id: 'eq.' + id });
  await db('DELETE', 'employee_schedules', { employee_id: 'eq.' + id });
  await db('DELETE', 'employees', { employee_id: 'eq.' + id });
  if (email) {
    await db('DELETE', 'notification_seen', { email: 'eq.' + email });
    await db('DELETE', 'config', { key: 'eq.' + authCredKey(email) });
  }
  await db('DELETE', 'config', { key: 'eq.' + profileExtraKey(id) });
  await db('DELETE', 'config', { key: 'eq.' + 'AUTH_ACTIVATION_OUTBOX_' + id });
  await auditLog(actorEmail, 'DELETE', 'employees', 'Hapus employee ' + id + ' (' + email + ')', String(ip || ''));
  return { ok: true, message: 'Employee berhasil dihapus.', employee_id: id, email: email, nama: String(row.nama || '') };
}
function toDataUrlFromFileObject(obj) {
  if (!obj || typeof obj !== 'object') return '';
  const base64 = String(obj.base64Data || '').trim();
  if (!base64) return '';
  const mime = String(obj.mimeType || 'application/octet-stream').trim();
  return 'data:' + mime + ';base64,' + base64;
}
function parseDriveFolderId(input) {
  const raw = String(input || '').trim();
  if (!raw) return '';
  const m1 = raw.match(/\/folders\/([a-zA-Z0-9_-]+)/);
  if (m1 && m1[1]) return m1[1];
  if (/^[a-zA-Z0-9_-]{10,}$/.test(raw)) return raw;
  return '';
}
function bufferFromBase64(base64) {
  const b = String(base64 || '').trim();
  if (!b) return null;
  if (typeof Buffer !== 'undefined') return Buffer.from(b, 'base64');
  return null;
}
function extensionFromMimeType(mimeType) {
  const m = String(mimeType || '').toLowerCase().trim();
  if (m === 'image/jpeg' || m === 'image/jpg') return 'jpg';
  if (m === 'image/png') return 'png';
  if (m === 'image/webp') return 'webp';
  if (m === 'image/gif') return 'gif';
  if (m === 'application/pdf') return 'pdf';
  return 'bin';
}
function toSafeFileToken(v, fallback) {
  const raw = String(v || '').trim();
  const t = raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 60);
  return t || String(fallback || 'file');
}
function driveFolderId() {
  const folderRaw = String(process.env.ATTENDANCE_DRIVE_FOLDER || '1cI6C6okEJDfQbfoIodJg2kUlDmMScRng').trim();
  return parseDriveFolderId(folderRaw);
}
function reportDriveFolderId() {
  const folderRaw = String(process.env.REPORT_DRIVE_FOLDER || '1kiRVtwgOrYDCTErhmhamzLPi_ID8jv5C').trim();
  return parseDriveFolderId(folderRaw);
}
function payrollDriveFolderId() {
  const folderRaw = String(process.env.PAYSLIP_DRIVE_FOLDER || '1uL6UT7PtjMPEGOW0MwsiEzwCtVCTisC6').trim();
  return parseDriveFolderId(folderRaw);
}
function leaveDriveFolderId() {
  const folderRaw = String(process.env.LEAVE_ATTACHMENT_DRIVE_FOLDER || '1mpbIt5CEkPOVFGd1MXHSh1VnDs2hwoKy').trim();
  return parseDriveFolderId(folderRaw);
}
function profilePhotoDriveFolderId() {
  const folderRaw = String(process.env.PROFILE_PHOTO_DRIVE_FOLDER || 'https://drive.google.com/drive/folders/1Z8QW_W_iAFvOiDfHfcMEjIgRc6I24woQ').trim();
  return parseDriveFolderId(folderRaw);
}
async function getDriveAccessToken() {
  const directToken = String(process.env.GOOGLE_DRIVE_ACCESS_TOKEN || '').trim();
  if (directToken) return { ok: true, token: directToken, source: 'access_token' };
  const refreshToken = String(process.env.GOOGLE_DRIVE_REFRESH_TOKEN || '').trim();
  const clientId = String(process.env.GOOGLE_OAUTH_CLIENT_ID || '').trim();
  const clientSecret = String(process.env.GOOGLE_OAUTH_CLIENT_SECRET || '').trim();
  if (!refreshToken || !clientId || !clientSecret) return { ok: false, error: 'GOOGLE_DRIVE_ACCESS_TOKEN atau (GOOGLE_DRIVE_REFRESH_TOKEN + GOOGLE_OAUTH_CLIENT_ID + GOOGLE_OAUTH_CLIENT_SECRET) belum diset.' };
  const body = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    refresh_token: refreshToken,
    grant_type: 'refresh_token'
  });
  const r = await fetch('https://oauth2.googleapis.com/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: body.toString() });
  const tx = await r.text();
  let j = null;
  try { j = tx ? JSON.parse(tx) : null; } catch (e) { j = null; }
  const token = String((j && j.access_token) || '').trim();
  if (!r.ok || !token) return { ok: false, error: j || tx || 'Gagal refresh token Google OAuth.' };
  return { ok: true, token: token, source: 'refresh_token' };
}
async function tryUploadAttendancePhotoToDrive(photoObj, meta) {
  if (!photoObj || typeof photoObj !== 'object') return '';
  const tk = await getDriveAccessToken();
  const token = tk.ok ? tk.token : '';
  const folderId = driveFolderId();
  if (!token || !folderId) return '';
  const base64 = String(photoObj.base64Data || '').trim();
  const mimeType = String(photoObj.mimeType || 'image/jpeg').trim();
  const bin = bufferFromBase64(base64);
  if (!bin) return '';
  const employeeName = toSafeFileToken(meta && meta.employee_name, meta && meta.employee_id || 'karyawan');
  const tanggal = String(meta && meta.tanggal || ymd()).replace(/[^0-9]/g, '');
  const jam = String(meta && meta.jam || hms()).replace(/[^0-9]/g, '');
  const type = toSafeFileToken(meta && meta.type || 'checkin', 'checkin');
  const fileName = employeeName + '_' + tanggal + '_' + jam + '_' + type + '.jpg';
  const metadata = JSON.stringify({ name: fileName, parents: [folderId], mimeType: mimeType });
  const boundary = 'essBoundary' + Date.now().toString(36);
  const part1 = Buffer.from('--' + boundary + '\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n' + metadata + '\r\n');
  const part2 = Buffer.from('--' + boundary + '\r\nContent-Type: ' + mimeType + '\r\n\r\n');
  const part3 = Buffer.from('\r\n--' + boundary + '--');
  const body = Buffer.concat([part1, part2, bin, part3]);
  const up = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink,thumbnailLink', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'multipart/related; boundary=' + boundary },
    body: body
  });
  const upText = await up.text();
  let upJson = null;
  try { upJson = upText ? JSON.parse(upText) : null; } catch (e) { upJson = null; }
  if (!up.ok || !upJson || !upJson.id) return '';
  const fileId = String(upJson.id || '');
  if (!fileId) return '';
  await fetch('https://www.googleapis.com/drive/v3/files/' + fileId + '/permissions', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ role: 'reader', type: 'anyone' })
  });
  return 'https://drive.google.com/thumbnail?id=' + fileId + '&sz=w1200';
}
async function tryUploadLeaveAttachmentToDrive(fileObj, meta) {
  if (!fileObj || typeof fileObj !== 'object') return '';
  const tk = await getDriveAccessToken();
  const token = tk.ok ? tk.token : '';
  const folderId = leaveDriveFolderId();
  if (!token || !folderId) return '';
  const base64 = String(fileObj.base64Data || '').trim();
  const mimeType = String(fileObj.mimeType || 'application/octet-stream').trim();
  const bin = bufferFromBase64(base64);
  if (!bin) return '';
  const employeeName = toSafeFileToken(meta && meta.employee_name, meta && meta.employee_id || 'karyawan');
  const leaveType = toSafeFileToken(meta && meta.leave_type, 'cuti');
  const tanggal = String(meta && meta.tanggal || ymd()).replace(/[^0-9]/g, '');
  const jam = String(meta && meta.jam || hms()).replace(/[^0-9]/g, '');
  const ext = extensionFromMimeType(mimeType);
  const fileName = employeeName + '_' + leaveType + '_' + tanggal + '_' + jam + '.' + ext;
  const metadata = JSON.stringify({ name: fileName, parents: [folderId], mimeType: mimeType });
  const boundary = 'essBoundary' + Date.now().toString(36);
  const part1 = Buffer.from('--' + boundary + '\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n' + metadata + '\r\n');
  const part2 = Buffer.from('--' + boundary + '\r\nContent-Type: ' + mimeType + '\r\n\r\n');
  const part3 = Buffer.from('\r\n--' + boundary + '--');
  const body = Buffer.concat([part1, part2, bin, part3]);
  const up = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink,thumbnailLink,mimeType', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'multipart/related; boundary=' + boundary },
    body: body
  });
  const upText = await up.text();
  let upJson = null;
  try { upJson = upText ? JSON.parse(upText) : null; } catch (e) { upJson = null; }
  if (!up.ok || !upJson || !upJson.id) return '';
  const fileId = String(upJson.id || '');
  if (!fileId) return '';
  await fetch('https://www.googleapis.com/drive/v3/files/' + fileId + '/permissions', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ role: 'reader', type: 'anyone' })
  });
  return 'https://drive.google.com/file/d/' + fileId + '/view';
}
async function uploadBufferToDrive(fileName, mimeType, buffer, folderId) {
  const tk = await getDriveAccessToken();
  const token = tk.ok ? tk.token : '';
  const parentId = String(folderId || '').trim();
  if (!token || !parentId || !buffer) return '';
  const metadata = JSON.stringify({ name: String(fileName || 'report.txt'), parents: [parentId], mimeType: String(mimeType || 'application/octet-stream') });
  const boundary = 'essBoundary' + Date.now().toString(36);
  const part1 = Buffer.from('--' + boundary + '\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n' + metadata + '\r\n');
  const part2 = Buffer.from('--' + boundary + '\r\nContent-Type: ' + String(mimeType || 'application/octet-stream') + '\r\n\r\n');
  const part3 = Buffer.from('\r\n--' + boundary + '--');
  const body = Buffer.concat([part1, part2, buffer, part3]);
  const up = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'multipart/related; boundary=' + boundary },
    body: body
  });
  const tx = await up.text();
  let j = null;
  try { j = tx ? JSON.parse(tx) : null; } catch (e) { j = null; }
  const fileId = String((j && j.id) || '');
  if (!up.ok || !fileId) return '';
  await fetch('https://www.googleapis.com/drive/v3/files/' + fileId + '/permissions', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ role: 'reader', type: 'anyone' })
  });
  return 'https://drive.google.com/file/d/' + fileId + '/view';
}
function simplePdfBuffer(title, rows) {
  const header = String(title || 'ESS Report').replace(/[()]/g, '');
  const lines = [header].concat((rows || []).map(function(r) { return String(r || '').replace(/[()]/g, ''); }));
  let y = 800;
  const commands = [];
  lines.slice(0, 42).forEach(function(line) {
    commands.push('BT /F1 10 Tf 40 ' + String(y) + ' Td (' + line + ') Tj ET');
    y -= 16;
  });
  const content = commands.join('\n');
  const objects = [];
  objects.push('1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj');
  objects.push('2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj');
  objects.push('3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj');
  objects.push('4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj');
  objects.push('5 0 obj << /Length ' + String(Buffer.byteLength(content, 'utf8')) + ' >> stream\n' + content + '\nendstream endobj');
  let pdf = '%PDF-1.4\n';
  const offsets = [0];
  objects.forEach(function(obj) { offsets.push(Buffer.byteLength(pdf, 'utf8')); pdf += obj + '\n'; });
  const xrefPos = Buffer.byteLength(pdf, 'utf8');
  pdf += 'xref\n0 ' + String(objects.length + 1) + '\n';
  pdf += '0000000000 65535 f \n';
  for (let i = 1; i <= objects.length; i += 1) {
    pdf += String(offsets[i]).padStart(10, '0') + ' 00000 n \n';
  }
  pdf += 'trailer << /Size ' + String(objects.length + 1) + ' /Root 1 0 R >>\nstartxref\n' + String(xrefPos) + '\n%%EOF';
  return Buffer.from(pdf, 'utf8');
}
function pdfEscapeText(text) {
  return String(text || '').replace(/\\/g, '\\\\').replace(/\(/g, '\\(').replace(/\)/g, '\\)');
}
function pdfBufferFromContent(content) {
  const objects = [];
  objects.push('1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj');
  objects.push('2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj');
  objects.push('3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R /F2 6 0 R >> >> /Contents 5 0 R >> endobj');
  objects.push('4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj');
  objects.push('5 0 obj << /Length ' + String(Buffer.byteLength(content, 'utf8')) + ' >> stream\n' + content + '\nendstream endobj');
  objects.push('6 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >> endobj');
  let pdf = '%PDF-1.4\n';
  const offsets = [0];
  objects.forEach(function(obj) { offsets.push(Buffer.byteLength(pdf, 'utf8')); pdf += obj + '\n'; });
  const xrefPos = Buffer.byteLength(pdf, 'utf8');
  pdf += 'xref\n0 ' + String(objects.length + 1) + '\n';
  pdf += '0000000000 65535 f \n';
  for (let i = 1; i <= objects.length; i += 1) pdf += String(offsets[i]).padStart(10, '0') + ' 00000 n \n';
  pdf += 'trailer << /Size ' + String(objects.length + 1) + ' /Root 1 0 R >>\nstartxref\n' + String(xrefPos) + '\n%%EOF';
  return Buffer.from(pdf, 'utf8');
}
function payrollPdfBuffer(payrollDoc, employeeName) {
  const d = payrollDoc || {};
  const money = function(v) { return 'Rp ' + Number(toMoney(v)).toLocaleString('id-ID'); };
  const earn = ((d.breakdown && d.breakdown.earnings) || []).filter(function(x) { return Number(x.value || 0) !== 0; });
  const ded = ((d.breakdown && d.breakdown.deductions) || []).filter(function(x) { return Number(x.value || 0) !== 0; });
  const leftX = 36;
  const rightX = 305;
  let y = 805;
  const cmds = [];
  const text = function(x, yy, size, val, bold) {
    const font = bold ? '/F2' : '/F1';
    cmds.push('BT ' + font + ' ' + String(size) + ' Tf ' + String(x) + ' ' + String(yy) + ' Td (' + pdfEscapeText(val) + ') Tj ET');
  };
  const line = function(x1, y1, x2, y2) { cmds.push(String(x1) + ' ' + String(y1) + ' m ' + String(x2) + ' ' + String(y2) + ' l S'); };
  const box = function(x1, y1, w, h) { cmds.push(String(x1) + ' ' + String(y1) + ' ' + String(w) + ' ' + String(h) + ' re S'); };
  text(leftX, y, 18, 'TREND HORIZON', true);
  text(leftX, y - 20, 12, 'SLIP GAJI KARYAWAN', true);
  text(430, y - 4, 9, 'Generated: ' + String(nowIso()).slice(0, 19).replace('T', ' '), false);
  line(30, y - 30, 565, y - 30);
  y -= 52;
  text(leftX, y, 10, 'Periode Gaji', true); text(leftX + 95, y, 10, ': ' + String(d.bulan || '-') + ' ' + String(d.tahun || '-'), false);
  text(leftX, y - 16, 10, 'Employee ID', true); text(leftX + 95, y - 16, 10, ': ' + String(d.employee_id || '-'), false);
  text(leftX, y - 32, 10, 'Nama Karyawan', true); text(leftX + 95, y - 32, 10, ': ' + String(employeeName || '-'), false);
  text(rightX, y, 10, 'Total Pendapatan', true); text(rightX + 105, y, 10, ': ' + money(d.total_earning || 0), false);
  text(rightX, y - 16, 10, 'Total Potongan', true); text(rightX + 105, y - 16, 10, ': ' + money(d.total_deduction || 0), false);
  text(rightX, y - 32, 11, 'TAKE HOME PAY', true); text(rightX + 105, y - 32, 11, ': ' + money(d.net_salary || 0), true);
  y -= 52;
  line(30, y, 565, y);
  y -= 18;
  text(leftX, y, 11, 'PENDAPATAN', true);
  text(rightX, y, 11, 'POTONGAN', true);
  y -= 10;
  box(30, y - 410, 255, 410);
  box(300, y - 410, 265, 410);
  let ly = y - 18;
  let ry = y - 18;
  const rowGap = 14;
  if (!earn.length) { text(leftX, ly, 9, '- Tidak ada komponen pendapatan', false); ly -= rowGap; }
  earn.slice(0, 24).forEach(function(x) {
    text(leftX, ly, 9, String(x.name || '-'), false);
    text(245, ly, 9, money(x.value || 0), false);
    ly -= rowGap;
  });
  if (!ded.length) { text(rightX, ry, 9, '- Tidak ada komponen potongan', false); ry -= rowGap; }
  ded.slice(0, 24).forEach(function(x) {
    text(rightX, ry, 9, String(x.name || '-'), false);
    text(525, ry, 9, money(x.value || 0), false);
    ry -= rowGap;
  });
  text(36, 70, 9, 'Dokumen ini dibuat otomatis oleh sistem ESS Trend Horizon.', false);
  text(36, 56, 9, 'Jika ada perbedaan data, hubungi tim HR & Payroll.', false);
  return pdfBufferFromContent(cmds.join('\n'));
}
async function getEmployeeDisplayName(user) {
  const r = await db('GET', 'employees', { select: 'nama', employee_id: 'eq.' + String(user.employee_id || ''), limit: 1 });
  if (!r.ok) return String(user.employee_id || user.email || 'karyawan');
  const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
  return String((row && row.nama) || user.employee_id || user.email || 'karyawan');
}
function calcLeaveDays(startDate, endDate) {
  const a = new Date(startDate);
  const b = new Date(endDate);
  if (isNaN(a.getTime()) || isNaN(b.getTime())) return 0;
  const oneDay = 24 * 60 * 60 * 1000;
  return Math.max(0, Math.floor((b.setHours(0, 0, 0, 0) - a.setHours(0, 0, 0, 0)) / oneDay) + 1);
}
function appBaseUrl() {
  return String(process.env.APP_BASE_URL || 'https://ess-trendhorizone.space').trim().replace(/\/+$/, '');
}
async function sendEssEmail(toEmail, subject, html) {
  const to = String(toEmail || '').trim().toLowerCase();
  if (!to) return { sent: false, message: 'Email tujuan kosong.' };
  const apiKey = String(process.env.RESEND_API_KEY || process.env.RESEND_KEY || process.env.EMAIL_RESEND_API_KEY || '').trim();
  if (!apiKey) return { sent: false, message: 'RESEND_API_KEY belum diset.' };
  const base = appBaseUrl();
  const from = String(process.env.RESEND_FROM || ('ESS Trendhorizone <no-reply@' + String(new URL(base).hostname || 'trendhorizone.space') + '>')).trim();
  const fallbackFrom = String(process.env.RESEND_FROM_FALLBACK || 'Trendhorizone ESS <onboarding@resend.dev>').trim();
  const candidates = [from, fallbackFrom].filter(Boolean).filter(function(v, i, arr) { return arr.findIndex(function(x) { return String(x).toLowerCase() === String(v).toLowerCase(); }) === i; });
  let lastErr = '';
  for (const sender of candidates) {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + apiKey, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: sender, to: [to], subject: String(subject || ''), html: String(html || '') })
    });
    const tx = await r.text();
    if (r.ok) {
      let j = null; try { j = tx ? JSON.parse(tx) : null; } catch (_) { j = null; }
      return { sent: true, channel: 'resend', sender: sender, provider_id: String((j && j.id) || '') };
    }
    lastErr = tx;
  }
  return { sent: false, channel: 'resend', error: lastErr };
}
async function getAdminEmails() {
  const r = await db('GET', 'employees', { select: 'email,role,is_active', role: 'in.(admin,superadmin,manager)', is_active: 'eq.true', limit: 500 });
  if (!r.ok) return [];
  return Array.from(new Set((r.data || []).map(function(x) { return String(x.email || '').trim().toLowerCase(); }).filter(isValidEmail)));
}
function leaveMailTemplate(title, bodyLines) {
  const base = appBaseUrl();
  const lines = Array.isArray(bodyLines) ? bodyLines : [];
  return '<div style="font-family:Inter,Arial,sans-serif;background:#f8fafc;padding:24px;color:#0f172a;">'
    + '<div style="max-width:680px;margin:0 auto;background:#fff;border:1px solid #e2e8f0;border-radius:14px;overflow:hidden;">'
    + '<div style="padding:16px 20px;background:linear-gradient(135deg,#1d4ed8,#4f46e5);color:#fff;font-weight:700;">' + String(title || 'Notifikasi ESS') + '</div>'
    + '<div style="padding:18px 20px;">' + lines.map(function(x) { return '<p style="margin:0 0 10px;">' + String(x || '') + '</p>'; }).join('')
    + '<p style="margin:12px 0 0;">Akses sistem: <a href="' + base + '/login">' + base + '/login</a></p>'
    + '<p style="margin:10px 0 0;color:#64748b;font-size:12px;">Email ini dikirim otomatis oleh sistem ESS.</p></div></div></div>';
}
function dateOnly(v) {
  const d = new Date(v);
  if (isNaN(d.getTime())) return '';
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}
function dateShift(baseDate, deltaDays) {
  const d = new Date(baseDate);
  if (isNaN(d.getTime())) return '';
  d.setDate(d.getDate() + Number(deltaDays || 0));
  return dateOnly(d);
}
function dateRangeList(startDate, endDate) {
  const out = [];
  let cur = dateOnly(startDate);
  const end = dateOnly(endDate);
  if (!cur || !end) return out;
  while (cur <= end) {
    out.push(cur);
    cur = dateShift(cur, 1);
    if (!cur) break;
    if (out.length > 4000) break;
  }
  return out;
}
function safeJsonParse(v, fallback) {
  try { return JSON.parse(String(v || '')); } catch (e) { return fallback; }
}
function hammingDistance(a, b) {
  const x = String(a || '');
  const y = String(b || '');
  const len = Math.min(x.length, y.length);
  let diff = Math.abs(x.length - y.length);
  for (let i = 0; i < len; i += 1) if (x[i] !== y[i]) diff += 1;
  return diff;
}
function toSecondsFromHms(v) {
  const s = String(v || '').trim();
  const m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return 0;
  const h = Number(m[1] || 0);
  const mi = Number(m[2] || 0);
  const se = Number(m[3] || 0);
  return Math.max(0, (h * 3600) + (mi * 60) + se);
}
function workDurationMinutes(jamMasuk, jamKeluar) {
  return Math.floor(workDurationSeconds(jamMasuk, jamKeluar) / 60);
}
function workDurationSeconds(jamMasuk, jamKeluar) {
  const a = toSecondsFromHms(jamMasuk);
  const b = toSecondsFromHms(jamKeluar);
  if (!a || !b) return 0;
  const diff = b >= a ? (b - a) : ((24 * 3600 - a) + b);
  return Math.max(0, diff);
}
function workDurationLabel(minutes) {
  const m = Math.max(0, Number(minutes || 0));
  const h = Math.floor(m / 60);
  const r = m % 60;
  return String(h) + 'j ' + String(r) + 'm';
}
function workDurationDigital(seconds) {
  const s = Math.max(0, Math.floor(Number(seconds || 0)));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const se = s % 60;
  return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + ':' + String(se).padStart(2, '0');
}
function addMinutesToHms(hmsText, minutesToAdd) {
  const sec = toSecondsFromHms(hmsText);
  if (!sec) return '';
  const total = (sec + (Number(minutesToAdd || 0) * 60)) % (24 * 3600);
  const s = total < 0 ? total + (24 * 3600) : total;
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const se = s % 60;
  return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + ':' + String(se).padStart(2, '0');
}
function normalizeShiftCode(v) {
  const s = String(v || '').trim().toUpperCase();
  if (s === 'PAGI') return 'PAGI';
  if (s === 'SORE') return 'SORE';
  if (s === 'MALAM') return 'MALAM';
  if (s === 'FLX' || s === 'FLEXIBLE' || s === 'FLEXI') return 'FLX';
  return 'PAGI';
}
function shiftRule(shiftCode) {
  const c = normalizeShiftCode(shiftCode);
  if (c === 'SORE') return { code: 'SORE', start: '15:00:00', end: '01:00:00', break_minutes_default: 120, target_work_minutes: 8 * 60 };
  if (c === 'MALAM') return { code: 'MALAM', start: '21:00:00', end: '07:00:00', break_minutes_default: 120, target_work_minutes: 8 * 60 };
  if (c === 'FLX') return { code: 'FLX', start: '', end: '', break_minutes_default: 0, target_work_minutes: 5 * 60 };
  return { code: 'PAGI', start: '09:00:00', end: '19:00:00', break_minutes_default: 120, target_work_minutes: 8 * 60 };
}
function shiftLateAfterTime(shiftCode) {
  const r = shiftRule(shiftCode);
  if (!r.start) return '';
  return addMinutesToHms(r.start, 15);
}
function monthNameId(monthText) {
  const m = String(monthText || '').trim().match(/^(\d{4})-(\d{2})$/);
  if (!m) return String(monthText || '');
  const arr = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'];
  const idx = Math.max(1, Math.min(12, Number(m[2] || 1))) - 1;
  return arr[idx] + ' ' + m[1];
}
function dayNameId(dateText) {
  const d = new Date(String(dateText || ymd()));
  if (Number.isNaN(d.getTime())) return '';
  const names = ['Minggu', 'Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu'];
  return names[d.getDay()] || '';
}
async function resolveEmployeeShiftForDate(employeeId, dateText) {
  const date = String(dateText || ymd()).trim().slice(0, 10);
  const month = date.slice(0, 7);
  const key = 'SHIFT_SCHEDULE_' + month;
  const r = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
  if (!r.ok) return { shift_code: 'PAGI', off_day: false, month: month, published_at: '' };
  const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
  const value = row ? safeJsonParse(row.value, {}) : {};
  const templates = value.templates || {};
  const tpl = templates[String(employeeId || '')] || {};
  const shiftCode = normalizeShiftCode(tpl.shift_code || 'PAGI');
  const day = dayNameId(date);
  const offSaturday = String(tpl.off_saturday || '').toLowerCase() === 'true' || tpl.off_saturday === true;
  const offSunday = String(tpl.off_sunday || '').toLowerCase() === 'true' || tpl.off_sunday === true;
  const isOff = (day === 'Sabtu' && offSaturday) || (day === 'Minggu' && offSunday);
  return {
    shift_code: isOff ? 'OFF' : shiftCode,
    off_day: isOff,
    month: month,
    published_at: String(value.published_at || '')
  };
}
function normalizeScheduleRule(raw) {
  const r = raw || {};
  return {
    shift_code: normalizeShiftCode(String(r.shift_code || 'PAGI')),
    off_saturday: String(r.off_saturday || '').toLowerCase() === 'true' || r.off_saturday === true,
    off_sunday: String(r.off_sunday || '').toLowerCase() === 'true' || r.off_sunday === true
  };
}
function normalizeScheduleRulesMap(rawRules) {
  const src = (rawRules && typeof rawRules === 'object') ? rawRules : {};
  const out = {};
  Object.keys(src).forEach(function(k) {
    const key = String(k || '').trim();
    if (!key) return;
    out[key] = normalizeScheduleRule(src[key]);
  });
  return out;
}
async function readScheduleDefaultRules() {
  const key = 'SHIFT_DEFAULT_RULES';
  const r = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
  if (!r.ok) return { ok: false, message: 'Gagal membaca default schedule.', error: r.error, rules: {}, updated_at: '', updated_by: '' };
  const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
  const val = row ? safeJsonParse(row.value, {}) : {};
  return { ok: true, rules: normalizeScheduleRulesMap(val.rules || {}), updated_at: String(val.updated_at || ''), updated_by: String(val.updated_by || '') };
}
async function saveScheduleDefaultRules(rules, actorEmail) {
  const key = 'SHIFT_DEFAULT_RULES';
  const payload = {
    key: key,
    value: JSON.stringify({
      rules: normalizeScheduleRulesMap(rules),
      updated_at: nowIso(),
      updated_by: String(actorEmail || '')
    })
  };
  const up = await db('POST', 'config', { on_conflict: 'key' }, payload, { Prefer: 'resolution=merge-duplicates,return=minimal' });
  if (!up.ok) return { ok: false, message: 'Gagal menyimpan default schedule.', error: up.error };
  return { ok: true };
}
async function saveScheduleEmployeeRows(month, templates, actorEmail) {
  const mm = String(month || '').trim().slice(0, 7);
  const tm = (templates && typeof templates === 'object') ? templates : {};
  const prefix = 'SHIFT_SCHEDULE_EMP_' + mm + '_';
  const existing = await db('GET', 'config', { select: 'key', key: 'like.' + prefix + '%', limit: 5000 });
  if (!existing.ok) return { ok: false, message: 'Gagal membaca rows schedule employee.', error: existing.error };
  const existingKeys = new Set((Array.isArray(existing.data) ? existing.data : []).map(function(r) { return String(r.key || ''); }));
  const nextKeys = new Set();
  let upserted = 0;
  for (const empId of Object.keys(tm)) {
    const id = String(empId || '').trim();
    if (!id) continue;
    const key = prefix + id;
    nextKeys.add(key);
    const t = tm[empId] || {};
    const val = {
      month: mm,
      employee_id: id,
      shift_code: normalizeShiftCode(String(t.shift_code || 'PAGI')),
      off_saturday: !!t.off_saturday,
      off_sunday: !!t.off_sunday,
      updated_at: nowIso(),
      updated_by: String(actorEmail || '')
    };
    const up = await db('POST', 'config', { on_conflict: 'key' }, { key: key, value: JSON.stringify(val) }, { Prefer: 'resolution=merge-duplicates,return=minimal' });
    if (up.ok) upserted += 1;
  }
  let deleted = 0;
  for (const k of existingKeys) {
    if (nextKeys.has(k)) continue;
    const del = await db('DELETE', 'config', { key: 'eq.' + k });
    if (del.ok) deleted += 1;
  }
  return { ok: true, upserted: upserted, deleted: deleted, total: Object.keys(tm).length };
}
function computeScheduleDefaultsImpact(rules, templates, employees, overwrite) {
  const sourceRules = (rules && typeof rules === 'object') ? rules : {};
  const sourceTemplates = (templates && typeof templates === 'object') ? templates : {};
  const nextTemplates = Object.assign({}, sourceTemplates);
  const byDivisionMap = {};
  let applied = 0;
  let skippedNoRule = 0;
  let skippedExisting = 0;
  let skippedInactive = 0;
  for (const e of (Array.isArray(employees) ? employees : [])) {
    const active = String(e.is_active).toLowerCase() === 'true';
    if (!active) { skippedInactive += 1; continue; }
    const div = String(e.divisi || '').trim();
    const eid = String(e.employee_id || '').trim();
    if (!div || !eid || !sourceRules[div]) { skippedNoRule += 1; continue; }
    if (!byDivisionMap[div]) byDivisionMap[div] = { divisi: div, total_active: 0, applied: 0, skipped_existing: 0 };
    byDivisionMap[div].total_active += 1;
    if (!overwrite && sourceTemplates[eid]) { skippedExisting += 1; byDivisionMap[div].skipped_existing += 1; continue; }
    nextTemplates[eid] = normalizeScheduleRule(sourceRules[div]);
    applied += 1;
    byDivisionMap[div].applied += 1;
  }
  const byDivision = Object.values(byDivisionMap).sort(function(a, b) { return Number(b.applied || 0) - Number(a.applied || 0); });
  return { applied_count: applied, skipped_no_rule: skippedNoRule, skipped_existing: skippedExisting, skipped_inactive: skippedInactive, by_division: byDivision, templates: nextTemplates };
}
function attendanceMetaKey(attendanceId) {
  return 'ATTENDANCE_META_' + String(attendanceId || '');
}
async function attendanceMetaGet(attendanceId) {
  if (!attendanceId) return {};
  const r = await db('GET', 'config', { select: 'value', key: 'eq.' + attendanceMetaKey(attendanceId), limit: 1 });
  if (!r.ok) return {};
  const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
  if (!row) return {};
  return safeJsonParse(row.value, {}) || {};
}
async function attendanceMetaSave(attendanceId, meta) {
  if (!attendanceId) return false;
  const payload = { key: attendanceMetaKey(attendanceId), value: JSON.stringify(meta || {}) };
  const r = await db('POST', 'config', { on_conflict: 'key' }, payload, { Prefer: 'resolution=merge-duplicates,return=minimal' });
  return !!r.ok;
}
function effectiveWorkMinutes(jamMasuk, jamKeluar, meta, nowTime) {
  return Math.floor(effectiveWorkSeconds(jamMasuk, jamKeluar, meta, nowTime) / 60);
}
function effectiveWorkSeconds(jamMasuk, jamKeluar, meta, nowTime) {
  const base = workDurationSeconds(jamMasuk, jamKeluar || nowTime || hms());
  const m = meta || {};
  let breakSeconds = Number(m.break_total_seconds || (Number(m.break_total_minutes || 0) * 60) || 0);
  if (m.break_active_start && !jamKeluar) breakSeconds += workDurationSeconds(m.break_active_start, nowTime || hms());
  return Math.max(0, base - Math.max(0, breakSeconds));
}
function shiftProgress(effectiveMinutes, shiftCode) {
  const rule = shiftRule(shiftCode);
  const target = Number(rule.target_work_minutes || 0);
  const percent = target > 0 ? Math.min(100, Math.round((Math.max(0, effectiveMinutes) / target) * 100)) : 0;
  return { shift_code: rule.code, target_minutes: target, target_duration: workDurationLabel(target), progress_percent: percent };
}
async function auditLog(userEmail, aksi, modul, detail, ipInfo) {
  try {
    await db('POST', 'audit_log', null, {
      log_id: rid('LOG'),
      timestamp: nowIso(),
      user_email: String(userEmail || '').trim().toLowerCase(),
      aksi: String(aksi || '').trim(),
      modul: String(modul || '').trim(),
      detail: String(detail || '').trim(),
      ip_info: String(ipInfo || '').trim()
    }, { Prefer: 'return=minimal' });
  } catch (e) {}
}

function userCtx(req) {
  if (req && req._session_user && req._session_user.email) {
    return {
      email: String(req._session_user.email || '').trim().toLowerCase(),
      employee_id: String(req._session_user.employee_id || '').trim(),
      role: String(req._session_user.role || 'employee').trim().toLowerCase()
    };
  }
  const q = req.query || {};
  return {
    email: String(req.headers['x-user-email'] || q.email || '').trim().toLowerCase(),
    employee_id: String(req.headers['x-employee-id'] || q.employee_id || '').trim(),
    role: String(req.headers['x-user-role'] || q.role || 'employee').trim().toLowerCase()
  };
}
async function attachSessionUser(req) {
  const token = bearerToken(req) || String((req.query || {}).session_token || '').trim();
  if (!token) return null;
  const sess = await readSession(token);
  if (!sess) return null;
  const r = await db('GET', 'employees', { select: 'employee_id,email,role', employee_id: 'eq.' + String(sess.employee_id || ''), limit: 1 });
  if (!r.ok) return null;
  const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
  if (!row) return null;
  req._session_user = {
    email: String(row.email || '').trim().toLowerCase(),
    employee_id: String(row.employee_id || '').trim(),
    role: String(row.role || 'employee').trim().toLowerCase()
  };
  return req._session_user;
}

function requireUser(req, res) {
  const u = userCtx(req);
  if (!u.email || !u.employee_id) {
    json(res, 401, { ok: false, message: 'Header x-user-email dan x-employee-id wajib diisi.' });
    return null;
  }
  return u;
}

function requireAdmin(req, res) {
  const u = userCtx(req);
  if (!u.email) {
    json(res, 401, { ok: false, message: 'Header x-user-email wajib diisi.' });
    return null;
  }
  if (!roleAdmin(u.role)) {
    json(res, 403, { ok: false, message: 'Akses hanya untuk admin.' });
    return null;
  }
  return u;
}

function routePath(req) {
  const r = req.query && req.query.route;
  if (!r) {
    const rawUrl = String(req.url || '');
    const clean = rawUrl.split('?')[0];
    const marker = '/api/';
    const idx = clean.indexOf(marker);
    if (idx >= 0) {
      return clean.slice(idx + marker.length).replace(/^\/+/, '').replace(/\/+$/, '');
    }
    if (clean === '/api' || clean === '/api/') return '';
    return clean.replace(/^\/+/, '').replace(/\/+$/, '');
  }
  if (Array.isArray(r)) return r.join('/');
  return String(r);
}

async function handleMeAttendanceCheckIn(req, res, user) {
  const body = await readBody(req);
  const tanggal = String(body.tanggal || ymd()).trim();
  const openAny = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + user.employee_id, order: 'tanggal.desc,created_at.desc', limit: 120 });
  if (!openAny.ok) return json(res, 500, { ok: false, message: 'Gagal cek attendance aktif.', error: openAny.error });
  const openRow = (openAny.data || []).find(function(x) { return !!(x && x.jam_masuk) && !x.jam_keluar; }) || null;
  if (openRow && String(openRow.tanggal || '') !== tanggal) {
    return json(res, 400, { ok: false, message: 'Masih ada check-in aktif pada tanggal ' + String(openRow.tanggal || '-') + '. Silakan check-out dulu.' });
  }
  const existing = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + user.employee_id, tanggal: 'eq.' + tanggal, limit: 1, order: 'created_at.desc' });
  if (!existing.ok) return json(res, 500, { ok: false, message: 'Gagal cek attendance.', error: existing.error });
  const row = Array.isArray(existing.data) && existing.data[0] ? existing.data[0] : null;
  if (row && row.jam_masuk) return json(res, 400, { ok: false, message: 'Anda sudah check-in hari ini.' });

  const sched = await resolveEmployeeShiftForDate(user.employee_id, tanggal);
  const shift = String(body.shift_karyawan || sched.shift_code || '').trim();
  const shiftCode = shift === 'OFF' ? 'PAGI' : shiftRule(shift).code;
  const baseCatatan = String(body.catatan || '').trim();
  const jamMasuk = String(body.jam_masuk || hms()).trim();
  const lateAfter = shiftLateAfterTime(shiftCode);
  const statusAuto = (shiftCode === 'FLX') ? 'Hadir' : (lateAfter && jamMasuk > lateAfter ? 'Terlambat' : 'Hadir');
  const employeeName = await getEmployeeDisplayName(user);
  const sourcePhotoUrl = String(body.foto_masuk_url || '').trim() || toDataUrlFromFileObject(body.photo);
  const drivePhotoUrl = await tryUploadAttendancePhotoToDrive(body.photo, { type: 'checkin', employee_id: user.employee_id, employee_name: employeeName, tanggal: tanggal, jam: jamMasuk });
  const payload = {
    attendance_id: rid('ATD'),
    employee_id: user.employee_id,
    email: user.email,
    tanggal: tanggal,
    jam_masuk: jamMasuk,
    status: statusAuto,
    lokasi: String(body.lokasi || '').trim(),
    work_mode: String(body.work_mode || '').trim().toLowerCase(),
    foto_masuk_url: drivePhotoUrl || sourcePhotoUrl,
    catatan: baseCatatan,
    created_at: nowIso(),
    updated_at: nowIso()
  };
  const ins = await db('POST', 'attendance', null, payload, { Prefer: 'return=representation' });
  if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal check-in.', error: ins.error });
  const inserted = Array.isArray(ins.data) && ins.data[0] ? ins.data[0] : null;
  if (inserted && inserted.attendance_id) {
    await attendanceMetaSave(inserted.attendance_id, {
      shift_code: shiftRule(shift).code,
      break_total_seconds: 0,
      break_total_minutes: 0,
      break_active_start: '',
      break_sessions: []
    });
  }
  return json(res, 200, { ok: true, message: 'Check-in berhasil.', data: ins.data });
}

async function handleMeAttendanceCheckOut(req, res, user) {
  const body = await readBody(req);
  const requestedDate = String(body.tanggal || ymd()).trim();
  const existing = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + user.employee_id, tanggal: 'eq.' + requestedDate, limit: 1, order: 'created_at.desc' });
  if (!existing.ok) return json(res, 500, { ok: false, message: 'Gagal cek attendance.', error: existing.error });
  let row = Array.isArray(existing.data) && existing.data[0] ? existing.data[0] : null;
  if (!row || !row.attendance_id || !row.jam_masuk || row.jam_keluar) {
    const hist = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + user.employee_id, order: 'tanggal.desc,created_at.desc', limit: 120 });
    if (!hist.ok) return json(res, 500, { ok: false, message: 'Gagal cari attendance aktif.', error: hist.error });
    row = (hist.data || []).find(function(x) { return !!(x && x.jam_masuk) && !x.jam_keluar; }) || null;
  }
  if (!row || !row.attendance_id || !row.jam_masuk) return json(res, 400, { ok: false, message: 'Tidak ada sesi check-in aktif yang bisa di-check-out.' });
  if (row.jam_keluar) return json(res, 400, { ok: false, message: 'Sesi ini sudah check-out.' });

  const jamKeluar = String(body.jam_keluar || hms()).trim();
  const employeeName = await getEmployeeDisplayName(user);
  const sourcePhotoUrl = String(body.foto_keluar_url || '').trim() || toDataUrlFromFileObject(body.photo);
  const drivePhotoUrl = await tryUploadAttendancePhotoToDrive(body.photo, { type: 'checkout', employee_id: user.employee_id, employee_name: employeeName, tanggal: String(row.tanggal || requestedDate), jam: jamKeluar });
  const meta = await attendanceMetaGet(row.attendance_id);
  if (meta.break_active_start) {
    const extraSec = workDurationSeconds(meta.break_active_start, jamKeluar);
    const extraMin = Math.floor(extraSec / 60);
    meta.break_total_seconds = Number(meta.break_total_seconds || 0) + extraSec;
    meta.break_total_minutes = Math.floor(Number(meta.break_total_seconds || 0) / 60);
    meta.break_sessions = Array.isArray(meta.break_sessions) ? meta.break_sessions : [];
    meta.break_sessions.push({ start: meta.break_active_start, end: jamKeluar, seconds: extraSec, minutes: extraMin });
    meta.break_active_start = '';
    await attendanceMetaSave(row.attendance_id, meta);
  }
  const patch = await db('PATCH', 'attendance', { attendance_id: 'eq.' + row.attendance_id }, {
    jam_keluar: jamKeluar,
    status: String(body.status || row.status || 'Hadir').trim(),
    lokasi: String(body.lokasi || row.lokasi || '').trim(),
    foto_keluar_url: drivePhotoUrl || sourcePhotoUrl,
    catatan: String(body.catatan || row.catatan || '').trim(),
    updated_at: nowIso()
  }, { Prefer: 'return=representation' });
  if (!patch.ok) return json(res, 500, { ok: false, message: 'Gagal check-out.', error: patch.error });
  return json(res, 200, { ok: true, message: 'Check-out berhasil.', data: patch.data });
}

module.exports = async function handler(req, res) {
  try {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PATCH,DELETE,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-user-email, x-user-role, x-employee-id');
    if (String(req.method || '').toUpperCase() === 'OPTIONS') return json(res, 200, { ok: true });
    const path = routePath(req);
    const method = String(req.method || 'GET').toUpperCase();

    if (path === 'health' && method === 'GET') {
      const resendKeyReady = !!String(process.env.RESEND_API_KEY || process.env.RESEND_KEY || process.env.EMAIL_RESEND_API_KEY || '').trim();
      const resendFromReady = !!String(process.env.RESEND_FROM || '').trim();
      return json(res, 200, { ok: true, service: 'ess-trendhorizone-api', supabase_ready: !!env(), resend_ready: resendKeyReady && resendFromReady, key_source: process.env.RESEND_API_KEY ? 'RESEND_API_KEY' : (process.env.RESEND_KEY ? 'RESEND_KEY' : (process.env.EMAIL_RESEND_API_KEY ? 'EMAIL_RESEND_API_KEY' : 'none')), timestamp: nowIso() });
    }
    const needsSession = path.startsWith('me/') || path.startsWith('admin/');
    if (needsSession) {
      const su = await attachSessionUser(req);
      if (!su) return json(res, 401, { ok: false, message: 'Session login tidak valid. Silakan login kembali.' });
    }

    if (path === 'auth/login' && method === 'POST') {
      const b = await readBody(req);
      const email = String(b.email || '').trim().toLowerCase();
      const password = String(b.password || '');
      if (!email || !password) return json(res, 400, { ok: false, message: 'Email dan password wajib diisi.' });
      const emp = await db('GET', 'employees', { select: '*', email: 'eq.' + email, limit: 1 });
      if (!emp.ok) return json(res, 500, { ok: false, message: 'Gagal validasi user.', error: emp.error });
      const user = Array.isArray(emp.data) && emp.data[0] ? emp.data[0] : null;
      if (!user) return json(res, 401, { ok: false, message: 'Email atau password tidak valid.' });
      const cred = await getAuthCredByEmail(email);
      if (!cred || !cred.password_hash) return json(res, 401, { ok: false, message: 'Akun belum diaktivasi. Hubungi admin untuk aktivasi password awal.' });
      if (String(cred.password_hash) !== hashSha256(password)) return json(res, 401, { ok: false, message: 'Email atau password tidak valid.' });
      const sess = await createSessionForUser(user, 12);
      if (!sess) return json(res, 500, { ok: false, message: 'Gagal membuat session login.' });
      await auditLog(email, 'LOGIN', 'auth', 'Login ESS ' + String(user.employee_id || ''), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, {
        ok: true,
        message: 'Login berhasil.',
        session_token: sess.token,
        expires_at: sess.expires_at,
        first_login_required: cred.must_change_password === true || String(cred.first_login_required).toLowerCase() === 'true',
        user: {
          employee_id: String(user.employee_id || ''),
          email: String(user.email || ''),
          nama: String(user.nama || ''),
          role: String(user.role || 'employee').toLowerCase(),
          is_admin: roleAdmin(user.role)
        }
      });
    }

    if (path === 'auth/logout' && method === 'POST') {
      const b = await readBody(req);
      const token = bearerToken(req) || String(b.session_token || '').trim();
      if (!token) return json(res, 200, { ok: true, message: 'Session sudah berakhir.' });
      await deleteSession(token);
      return json(res, 200, { ok: true, message: 'Logout berhasil.' });
    }

    if (path === 'auth/session/me' && method === 'GET') {
      const token = bearerToken(req) || String(req.query.session_token || '').trim();
      if (!token) return json(res, 200, { ok: true, authenticated: false, user: null });
      const sess = await readSession(token);
      if (!sess) return json(res, 200, { ok: true, authenticated: false, user: null });
      const r = await db('GET', 'employees', { select: '*', employee_id: 'eq.' + String(sess.employee_id || ''), limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil session user.', error: r.error });
      const user = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
      if (!user) return json(res, 200, { ok: true, authenticated: false, user: null });
      const cred = await getAuthCredByEmail(String(user.email || ''));
      const extraRes = await db('GET', 'config', { select: 'value', key: 'eq.' + profileExtraKey(user.employee_id), limit: 1 });
      const extraRow = extraRes.ok && Array.isArray(extraRes.data) && extraRes.data[0] ? extraRes.data[0] : null;
      const extra = extraRow ? safeJsonParse(extraRow.value, {}) : {};
      return json(res, 200, {
        ok: true,
        authenticated: true,
        user: Object.assign({}, user, { photo_url: String(extra.photo_url || ''), is_admin: roleAdmin(user.role) }),
        first_login_required: !!(cred && (cred.must_change_password === true || String(cred.first_login_required).toLowerCase() === 'true')),
        session_expires_at: String(sess.expires_at || '')
      });
    }

    if (path === 'auth/me' && method === 'GET') {
      const u = userCtx(req);
      if (!u.email || !u.employee_id) return json(res, 200, { ok: true, authenticated: false, user: null });
      const r = await db('GET', 'employees', { select: '*', employee_id: 'eq.' + u.employee_id, limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil auth me.', error: r.error });
      return json(res, 200, { ok: true, authenticated: !!(r.data && r.data[0]), user: (r.data && r.data[0]) || null });
    }

    if (path === 'me/profile' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const r = await db('GET', 'employees', { select: '*', employee_id: 'eq.' + u.employee_id, limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil profile.', error: r.error });
      const extraRes = await db('GET', 'config', { select: 'value', key: 'eq.' + profileExtraKey(u.employee_id), limit: 1 });
      const extraRow = extraRes.ok && Array.isArray(extraRes.data) && extraRes.data[0] ? extraRes.data[0] : null;
      const extra = extraRow ? safeJsonParse(extraRow.value, {}) : {};
      const row = (r.data && r.data[0]) || null;
      if (!row) {
        return json(res, 200, {
          employee_id: u.employee_id,
          email: u.email,
          nama: 'Karyawan',
          role: u.role || 'employee',
          divisi: '-',
          jabatan: '-',
          photo_url: String(extra.photo_url || ''),
          first_login_completed: !!extra.first_login_completed,
          is_active: true,
          jatah_cuti: 12,
          sisa_cuti: 12
        });
      }
      return json(res, 200, Object.assign({}, row, {
        photo_url: String(extra.photo_url || ''),
        first_login_completed: !!extra.first_login_completed
      }));
    }

    if (path === 'me/profile' && method === 'PATCH') {
      const u = requireUser(req, res); if (!u) return;
      const b = await readBody(req);
      const patch = { updated_at: nowIso() };
      const allowed = ['nama', 'no_hp', 'alamat', 'tempat_lahir', 'tanggal_lahir', 'jenis_kelamin', 'npwp', 'bpjs', 'bank', 'no_rekening'];
      allowed.forEach(function(k) { if (b[k] !== undefined) patch[k] = String(b[k] || '').trim(); });
      if (b.tanggal_lahir !== undefined) patch.tanggal_lahir = b.tanggal_lahir || null;
      if (Object.keys(patch).length > 1) {
        const upd = await db('PATCH', 'employees', { employee_id: 'eq.' + u.employee_id }, patch, { Prefer: 'return=representation' });
        if (!upd.ok) return json(res, 500, { ok: false, message: 'Gagal update profile.', error: upd.error });
      }
      const extraRes = await db('GET', 'config', { select: 'value', key: 'eq.' + profileExtraKey(u.employee_id), limit: 1 });
      const extraRow = extraRes.ok && Array.isArray(extraRes.data) && extraRes.data[0] ? extraRes.data[0] : null;
      const extra = extraRow ? safeJsonParse(extraRow.value, {}) : {};
      if (b.photo && String((b.photo || {}).base64Data || '').trim()) {
        const n = (await db('GET', 'employees', { select: 'nama', employee_id: 'eq.' + u.employee_id, limit: 1 }));
        const nm = n.ok && Array.isArray(n.data) && n.data[0] ? String(n.data[0].nama || u.employee_id || 'karyawan') : String(u.employee_id || 'karyawan');
        const buf = Buffer.from(String(b.photo.base64Data || ''), 'base64');
        const mime = String((b.photo || {}).mimeType || 'image/jpeg');
        const ext = mime.indexOf('png') >= 0 ? 'png' : mime.indexOf('webp') >= 0 ? 'webp' : 'jpg';
        const fileName = toSafeFileToken('profile_' + nm + '_' + Date.now(), 'profile') + '.' + ext;
        const url = await uploadBufferToDrive(fileName, mime, buf, profilePhotoDriveFolderId());
        if (!url) return json(res, 500, { ok: false, message: 'Gagal upload photo profile ke folder Drive perusahaan. Pastikan folder dibagikan ke akun service dan kredensial Drive aktif.' });
        extra.photo_url = url;
      }
      if (b.new_password !== undefined) {
        const newPassword = String(b.new_password || '');
        if (newPassword && newPassword.length < 8) return json(res, 400, { ok: false, message: 'Password minimal 8 karakter.' });
        if (newPassword) {
          await upsertAuthCred(u.email, {
            employee_id: u.employee_id,
            password_hash: hashSha256(newPassword),
            must_change_password: false,
            first_login_required: false,
            password_last_set_at: nowIso()
          });
          extra.first_login_completed = true;
        }
      }
      if (b.first_login_completed !== undefined) extra.first_login_completed = String(b.first_login_completed).toLowerCase() === 'true' || b.first_login_completed === true;
      const extraUp = await db('POST', 'config', { on_conflict: 'key' }, { key: profileExtraKey(u.employee_id), value: JSON.stringify(extra) }, { Prefer: 'resolution=merge-duplicates,return=minimal' });
      if (!extraUp.ok) return json(res, 500, { ok: false, message: 'Gagal simpan profil tambahan.', error: extraUp.error });
      const out = await db('GET', 'employees', { select: '*', employee_id: 'eq.' + u.employee_id, limit: 1 });
      if (!out.ok) return json(res, 500, { ok: false, message: 'Profile berhasil disimpan, tapi gagal ambil data terbaru.', error: out.error });
      const row = Array.isArray(out.data) && out.data[0] ? out.data[0] : {};
      return json(res, 200, { ok: true, message: 'Profile berhasil diperbarui.', data: Object.assign({}, row, { photo_url: String(extra.photo_url || ''), first_login_completed: !!extra.first_login_completed }) });
    }

    if (path === 'me/face/profile' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const key = 'FACE_PROFILE_' + u.employee_id;
      const r = await db('GET', 'config', { select: 'key,value', key: 'eq.' + key, limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil profil wajah.', error: r.error });
      const row = (r.data && r.data[0]) || null;
      if (!row) return json(res, 200, { ok: true, enrolled: false, employee_id: u.employee_id });
      const value = safeJsonParse(row.value, {});
      const hashes = Array.isArray(value.face_hashes) ? value.face_hashes.filter(Boolean) : (value.face_hash ? [value.face_hash] : []);
      return json(res, 200, {
        ok: true,
        enrolled: hashes.length > 0,
        employee_id: u.employee_id,
        face_hash: String((hashes[0] || value.face_hash || '')),
        hash_count: hashes.length,
        face_photo_url: String(value.face_photo_url || ''),
        updated_at: ''
      });
    }

    if (path === 'me/face/enroll' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      const b = await readBody(req);
      const faceHash = String(b.face_hash || '').trim();
      const facePhotoUrl = String(b.face_photo_url || '').trim() || toDataUrlFromFileObject(b.photo);
      if (!faceHash) return json(res, 400, { ok: false, message: 'face_hash wajib diisi.' });
      const key = 'FACE_PROFILE_' + u.employee_id;
      const value = JSON.stringify({
        employee_id: u.employee_id,
        email: u.email,
        face_hash: faceHash,
        face_hashes: [faceHash],
        face_photo_url: facePhotoUrl,
        enrolled_at: nowIso()
      });
      const up = await db('POST', 'config', { on_conflict: 'key' }, { key: key, value: value }, { Prefer: 'resolution=merge-duplicates,return=representation' });
      if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal menyimpan profil wajah.', error: up.error });
      await auditLog(u.email, 'ENROLL_FACE', 'attendance_face', 'Enroll wajah ' + u.employee_id, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Profil wajah berhasil disimpan.', enrolled: true });
    }

    if (path === 'me/face/verify' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      const b = await readBody(req);
      const faceHash = String(b.face_hash || '').trim();
      if (!faceHash) return json(res, 400, { ok: false, message: 'face_hash wajib diisi.' });
      const key = 'FACE_PROFILE_' + u.employee_id;
      const r = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal verifikasi wajah.', error: r.error });
      const row = (r.data && r.data[0]) || null;
      if (!row) return json(res, 200, { ok: true, enrolled: false, verified: false, status: 'not_enrolled', color: 'red', similarity: 0 });
      const value = safeJsonParse(row.value, {});
      const enrolledHashes = Array.isArray(value.face_hashes) ? value.face_hashes.filter(Boolean).map(function(x) { return String(x); }) : [];
      if (value.face_hash && enrolledHashes.length === 0) enrolledHashes.push(String(value.face_hash));
      if (enrolledHashes.length === 0) return json(res, 200, { ok: true, enrolled: false, verified: false, status: 'not_enrolled', color: 'red', similarity: 0 });
      let best = 0;
      enrolledHashes.forEach(function(h) {
        const distance = hammingDistance(h, faceHash);
        const maxLen = Math.max(h.length, faceHash.length, 1);
        const sim = Math.max(0, 1 - (distance / maxLen));
        if (sim > best) best = sim;
      });
      const verified = best >= 0.9;
      return json(res, 200, {
        ok: true,
        enrolled: true,
        verified: verified,
        status: verified ? 'verified' : 'not_match',
        color: verified ? 'green' : 'red',
        similarity: Number(best.toFixed(4)),
        threshold: 0.9
      });
    }

    if (path === 'me/dashboard-summary' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const p = await db('GET', 'employees', { select: 'employee_id,nama,divisi,jabatan,jatah_cuti,sisa_cuti', employee_id: 'eq.' + u.employee_id, limit: 1 });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal ambil summary.', error: p.error });
      const row = (p.data && p.data[0]) || null;
      const profile = row || {
        employee_id: u.employee_id,
        nama: 'Karyawan',
        divisi: '-',
        jabatan: '-',
        jatah_cuti: 12,
        sisa_cuti: 12
      };
      const leaves = await db('GET', 'leave_requests', { select: 'leave_id', employee_id: 'eq.' + u.employee_id, status: 'eq.pending' });
      if (!leaves.ok) return json(res, 500, { ok: false, message: 'Gagal ambil pending leaves.', error: leaves.error });
      const att = await db('GET', 'attendance', { select: 'attendance_id,jam_masuk,jam_keluar', employee_id: 'eq.' + u.employee_id, tanggal: 'eq.' + ymd(), order: 'created_at.desc', limit: 1 });
      if (!att.ok) return json(res, 500, { ok: false, message: 'Gagal ambil attendance summary.', error: att.error });
      const arow = Array.isArray(att.data) && att.data[0] ? att.data[0] : null;
      const meta = arow && arow.attendance_id ? await attendanceMetaGet(arow.attendance_id) : {};
      const workSecondsToday = arow ? effectiveWorkSeconds(arow.jam_masuk, arow.jam_keluar, meta, hms()) : 0;
      const workMinutesToday = Math.floor(workSecondsToday / 60);
      const scheduleNow = await resolveEmployeeShiftForDate(u.employee_id, ymd());
      const scheduleShift = scheduleNow.shift_code === 'OFF' ? 'OFF' : normalizeShiftCode(scheduleNow.shift_code || 'PAGI');
      const progress = shiftProgress(workMinutesToday, meta.shift_code || scheduleShift || 'PAGI');
      return json(res, 200, Object.assign({}, profile, {
        pendingLeaves: Array.isArray(leaves.data) ? leaves.data.length : 0,
        work_seconds_today: workSecondsToday,
        work_minutes_today: workMinutesToday,
        work_duration_digital_today: workDurationDigital(workSecondsToday),
        work_duration_today: workDurationLabel(workMinutesToday),
        shift_code_today: scheduleShift === 'OFF' ? 'OFF' : progress.shift_code,
        shift_target_today: progress.target_duration,
        shift_progress_percent_today: scheduleShift === 'OFF' ? 0 : progress.progress_percent,
        shift_published_at_today: scheduleNow.published_at || ''
      }));
    }

    if (path === 'me/attendance/config' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const r = await db('GET', 'config', { select: 'key,value', key: 'in.(WORK_START_TIME,LATE_AFTER_TIME,PHOTO_FOLDER_ENABLED)' });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil config.', error: r.error });
      const map = {};
      (r.data || []).forEach(function(x) { map[String(x.key || '')] = String(x.value || ''); });
      return json(res, 200, { work_start_time: map.WORK_START_TIME || '08:00:00', late_after_time: map.LATE_AFTER_TIME || '08:30:00', photo_folder_enabled: String(map.PHOTO_FOLDER_ENABLED || 'true').toLowerCase() === 'true' });
    }

    if (path === 'me/attendance/today' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const tanggal = String(req.query.tanggal || ymd());
      const r = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + u.employee_id, tanggal: 'eq.' + tanggal, order: 'created_at.desc', limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil attendance today.', error: r.error });
      const row = (r.data && r.data[0]) || null;
      if (!row) {
        const openHist = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + u.employee_id, order: 'tanggal.desc,created_at.desc', limit: 120 });
        if (!openHist.ok) return json(res, 500, { ok: false, message: 'Gagal ambil attendance aktif.', error: openHist.error });
        const openRow = (openHist.data || []).find(function(x) { return !!(x && x.jam_masuk) && !x.jam_keluar; }) || null;
        if (openRow) {
          const metaOpen = await attendanceMetaGet(openRow.attendance_id);
          const workSecondsOpen = effectiveWorkSeconds(openRow.jam_masuk, openRow.jam_keluar, metaOpen, hms());
          const workMinutesOpen = Math.floor(workSecondsOpen / 60);
          const breakTotalSecondsOpen = Number(metaOpen.break_total_seconds || (Number(metaOpen.break_total_minutes || 0) * 60) || 0);
          const schedOpen = await resolveEmployeeShiftForDate(u.employee_id, String(openRow.tanggal || tanggal));
          const shiftCodeOpen = schedOpen.shift_code === 'OFF' ? 'PAGI' : normalizeShiftCode(schedOpen.shift_code || 'PAGI');
          const progressOpen = shiftProgress(workMinutesOpen, metaOpen.shift_code || shiftCodeOpen);
          return json(res, 200, Object.assign({}, openRow, {
            status: 'Sedang Kerja',
            cross_day_active: String(openRow.tanggal || '') !== String(tanggal || ''),
            current_date: tanggal,
            work_seconds: workSecondsOpen,
            work_minutes: workMinutesOpen,
            work_duration_digital: workDurationDigital(workSecondsOpen),
            work_duration: workDurationLabel(workMinutesOpen),
            break_total_seconds: breakTotalSecondsOpen,
            break_total_minutes: Number(metaOpen.break_total_minutes || 0),
            break_duration_digital: workDurationDigital(breakTotalSecondsOpen),
            break_active: !!metaOpen.break_active_start,
            break_active_start: String(metaOpen.break_active_start || ''),
            shift_code: progressOpen.shift_code,
            shift_target_duration: progressOpen.target_duration,
            shift_progress_percent: progressOpen.progress_percent,
            shift_late_after: shiftLateAfterTime(progressOpen.shift_code),
            shift_published_at: schedOpen.published_at || ''
          }));
        }
        const leaveToday = await db('GET', 'leave_requests', {
          select: 'leave_id,jenis_cuti,tanggal_mulai,tanggal_selesai,status',
          employee_id: 'eq.' + u.employee_id,
          status: 'eq.approved',
          and: '(tanggal_mulai.lte.' + tanggal + ',tanggal_selesai.gte.' + tanggal + ')',
          order: 'updated_at.desc,created_at.desc',
          limit: 1
        });
        const leaveRow = leaveToday.ok && Array.isArray(leaveToday.data) && leaveToday.data[0] ? leaveToday.data[0] : null;
        const sched = await resolveEmployeeShiftForDate(u.employee_id, tanggal);
        const isOff = sched.shift_code === 'OFF' || sched.off_day;
        return json(res, 200, {
          attendance_id: '',
          employee_id: u.employee_id,
          email: u.email,
          tanggal: tanggal,
          jam_masuk: '',
          jam_keluar: '',
          status: leaveRow ? 'On Leave' : (isOff ? 'Off Day' : 'Belum Absen'),
          lokasi: '',
          work_mode: '',
          foto_masuk_url: '',
          foto_keluar_url: '',
          catatan: '',
          on_leave: !!leaveRow,
          leave_id: leaveRow ? String(leaveRow.leave_id || '') : '',
          leave_type: leaveRow ? String(leaveRow.jenis_cuti || '') : '',
          work_seconds: 0,
          work_minutes: 0,
          work_duration_digital: '00:00:00',
          work_duration: '0j 0m',
          break_total_seconds: 0,
          break_total_minutes: 0,
          break_duration_digital: '00:00:00',
          break_active: false,
          shift_code: isOff ? 'OFF' : normalizeShiftCode(sched.shift_code || 'PAGI'),
          shift_target_duration: isOff ? '0j 0m' : shiftRule(sched.shift_code || 'PAGI').target_work_minutes ? workDurationLabel(shiftRule(sched.shift_code || 'PAGI').target_work_minutes) : '8j 0m',
          shift_progress_percent: 0,
          shift_late_after: isOff ? '' : shiftLateAfterTime(sched.shift_code || 'PAGI'),
          shift_published_at: sched.published_at || ''
        });
      }
      const meta = await attendanceMetaGet(row.attendance_id);
      const workSeconds = effectiveWorkSeconds(row.jam_masuk, row.jam_keluar, meta, hms());
      const workMinutes = Math.floor(workSeconds / 60);
      const breakTotalSeconds = Number(meta.break_total_seconds || (Number(meta.break_total_minutes || 0) * 60) || 0);
      const progress = shiftProgress(workMinutes, meta.shift_code || 'PAGI');
      return json(res, 200, Object.assign({}, row, {
        work_seconds: workSeconds,
        work_minutes: workMinutes,
        work_duration_digital: workDurationDigital(workSeconds),
        work_duration: workDurationLabel(workMinutes),
        break_total_seconds: breakTotalSeconds,
        break_total_minutes: Number(meta.break_total_minutes || 0),
        break_duration_digital: workDurationDigital(breakTotalSeconds),
        break_active: !!meta.break_active_start,
        break_active_start: String(meta.break_active_start || ''),
        shift_code: progress.shift_code,
        shift_target_duration: progress.target_duration,
        shift_progress_percent: progress.progress_percent,
        shift_late_after: shiftLateAfterTime(progress.shift_code)
      }));
    }

    if (path === 'me/attendance/history' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const r = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + u.employee_id, order: 'tanggal.desc,created_at.desc', limit: Math.min(Number(req.query.limit || 60), 365) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil history.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'me/attendance/check-in' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      return await handleMeAttendanceCheckIn(req, res, u);
    }

    if (path === 'me/attendance/check-out' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      return await handleMeAttendanceCheckOut(req, res, u);
    }

    if (path === 'me/attendance/break/start' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      const tanggal = String((await readBody(req)).tanggal || ymd()).trim();
      const r = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + u.employee_id, tanggal: 'eq.' + tanggal, order: 'created_at.desc', limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil attendance.', error: r.error });
      let row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
      if (!row || !row.jam_masuk || row.jam_keluar) {
        const hist = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + u.employee_id, order: 'tanggal.desc,created_at.desc', limit: 120 });
        if (!hist.ok) return json(res, 500, { ok: false, message: 'Gagal cari sesi attendance aktif.', error: hist.error });
        row = (hist.data || []).find(function(x) { return !!(x && x.jam_masuk) && !x.jam_keluar; }) || null;
      }
      if (!row || !row.jam_masuk) return json(res, 400, { ok: false, message: 'Tidak ada sesi check-in aktif.' });
      if (row.jam_keluar) return json(res, 400, { ok: false, message: 'Sudah check-out.' });
      const meta = await attendanceMetaGet(row.attendance_id);
      if (meta.break_active_start) return json(res, 400, { ok: false, message: 'Istirahat sudah berjalan.' });
      meta.shift_code = meta.shift_code || shiftRule('').code;
      meta.break_total_seconds = Number(meta.break_total_seconds || (Number(meta.break_total_minutes || 0) * 60) || 0);
      meta.break_total_minutes = Number(meta.break_total_minutes || 0);
      meta.break_sessions = Array.isArray(meta.break_sessions) ? meta.break_sessions : [];
      meta.break_active_start = hms();
      await attendanceMetaSave(row.attendance_id, meta);
      return json(res, 200, { ok: true, message: 'Istirahat dimulai.', break_active_start: meta.break_active_start, break_total_seconds: meta.break_total_seconds, break_total_minutes: meta.break_total_minutes });
    }

    if (path === 'me/attendance/break/end' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      const tanggal = String((await readBody(req)).tanggal || ymd()).trim();
      const r = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + u.employee_id, tanggal: 'eq.' + tanggal, order: 'created_at.desc', limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil attendance.', error: r.error });
      let row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
      if (!row || !row.jam_masuk || row.jam_keluar) {
        const hist = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + u.employee_id, order: 'tanggal.desc,created_at.desc', limit: 120 });
        if (!hist.ok) return json(res, 500, { ok: false, message: 'Gagal cari sesi attendance aktif.', error: hist.error });
        row = (hist.data || []).find(function(x) { return !!(x && x.jam_masuk) && !x.jam_keluar; }) || null;
      }
      if (!row || !row.jam_masuk) return json(res, 400, { ok: false, message: 'Tidak ada sesi check-in aktif.' });
      if (row.jam_keluar) return json(res, 400, { ok: false, message: 'Sudah check-out.' });
      const meta = await attendanceMetaGet(row.attendance_id);
      if (!meta.break_active_start) return json(res, 400, { ok: false, message: 'Istirahat tidak aktif.' });
      const nowT = hms();
      const sec = workDurationSeconds(meta.break_active_start, nowT);
      meta.break_total_seconds = Number(meta.break_total_seconds || (Number(meta.break_total_minutes || 0) * 60) || 0) + sec;
      meta.break_total_minutes = Math.floor(meta.break_total_seconds / 60);
      meta.break_sessions = Array.isArray(meta.break_sessions) ? meta.break_sessions : [];
      meta.break_sessions.push({ start: meta.break_active_start, end: nowT, seconds: sec, minutes: Math.floor(sec / 60) });
      meta.break_active_start = '';
      await attendanceMetaSave(row.attendance_id, meta);
      return json(res, 200, { ok: true, message: 'Istirahat selesai.', break_seconds_added: sec, break_minutes_added: Math.floor(sec / 60), break_total_seconds: meta.break_total_seconds, break_total_minutes: meta.break_total_minutes });
    }

    if (path === 'me/schedule/monthly' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const month = String(req.query.month || ymd().slice(0, 7)).trim().slice(0, 7);
      const key = 'SHIFT_SCHEDULE_' + month;
      const r = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil jadwal bulanan.', error: r.error });
      const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
      const value = row ? safeJsonParse(row.value, {}) : {};
      const templates = value.templates || {};
      const t = templates[u.employee_id] || {};
      const shiftCode = normalizeShiftCode(t.shift_code || 'PAGI');
      const offList = [];
      if (String(t.off_saturday || '').toLowerCase() === 'true' || t.off_saturday === true) offList.push('Sabtu');
      if (String(t.off_sunday || '').toLowerCase() === 'true' || t.off_sunday === true) offList.push('Minggu');
      const offText = offList.length ? offList.join(', ') : '-';
      return json(res, 200, {
        ok: true,
        month: month,
        employee_id: u.employee_id,
        shift_code: shiftCode,
        off_saturday: offList.indexOf('Sabtu') >= 0,
        off_sunday: offList.indexOf('Minggu') >= 0,
        summary_text: 'Jadwal Bulan ' + monthNameId(month) + ': ' + shiftCode + ', Off: ' + offText,
        published_at: value.published_at || '',
        notes: value.notes || ''
      });
    }

    if (path === 'me/schedule/watch' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const tanggal = String(req.query.tanggal || ymd()).trim().slice(0, 10);
      const resolved = await resolveEmployeeShiftForDate(u.employee_id, tanggal);
      const shiftCode = String(resolved.shift_code || 'PAGI');
      const publishedAt = String(resolved.published_at || '');
      const version = shiftCode + '|' + publishedAt + '|' + tanggal;
      return json(res, 200, {
        ok: true,
        tanggal: tanggal,
        employee_id: u.employee_id,
        shift_code: shiftCode,
        off_day: !!resolved.off_day,
        published_at: publishedAt,
        version: version
      });
    }

    if (path === 'leave-types/active' && method === 'GET') {
      const r = await db('GET', 'leave_types', { select: '*', is_active: 'eq.true', order: 'nama_jenis_cuti.asc' });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil leave types.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'me/leaves' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const r = await db('GET', 'leave_requests', { select: '*', employee_id: 'eq.' + u.employee_id, order: 'created_at.desc', limit: Math.min(Number(req.query.limit || 100), 365) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil leaves.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'me/leaves' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      const b = await readBody(req);
      const leaveTypeName = String(b.jenis_cuti || '').trim();
      const lt = leaveTypeName ? await db('GET', 'leave_types', { select: 'leave_type_id,nama_jenis_cuti,requires_attachment', nama_jenis_cuti: 'eq.' + leaveTypeName, limit: 1 }) : { ok: true, data: [] };
      if (!lt.ok) return json(res, 500, { ok: false, message: 'Gagal validasi jenis cuti.', error: lt.error });
      const ltRow = Array.isArray(lt.data) && lt.data[0] ? lt.data[0] : null;
      if (!ltRow) return json(res, 400, { ok: false, message: 'Jenis cuti tidak valid atau tidak aktif.' });
      const inferredSick = /sakit/i.test(leaveTypeName);
      const requireAttachment = !!((ltRow && (ltRow.requires_attachment === true || String(ltRow.requires_attachment).toLowerCase() === 'true')) || inferredSick);
      const sourceAttachmentUrl = String(b.lampiran_url || '').trim() || toDataUrlFromFileObject(b.attachment);
      const hasAttachmentObj = !!(b.attachment && String((b.attachment || {}).base64Data || '').trim());
      if (requireAttachment && !sourceAttachmentUrl && !hasAttachmentObj) {
        return json(res, 400, { ok: false, message: 'Lampiran surat wajib untuk cuti sakit.' });
      }
      const employeeName = await getEmployeeDisplayName(u);
      const driveAttachmentUrl = hasAttachmentObj ? await tryUploadLeaveAttachmentToDrive(b.attachment, {
        employee_id: u.employee_id,
        employee_name: employeeName,
        leave_type: leaveTypeName || 'cuti',
        tanggal: String(b.tanggal_mulai || ymd()),
        jam: hms()
      }) : '';
      if (requireAttachment && hasAttachmentObj && !driveAttachmentUrl) {
        return json(res, 500, { ok: false, message: 'Upload lampiran surat sakit ke Drive gagal. Periksa koneksi Google Drive.' });
      }
      const attachmentUrl = String(driveAttachmentUrl || sourceAttachmentUrl || '').trim();
      const days = Number(b.jumlah_hari || 0) > 0 ? Number(b.jumlah_hari || 0) : calcLeaveDays(b.tanggal_mulai, b.tanggal_selesai);
      const payload = {
        leave_id: rid('LEAVE'),
        employee_id: u.employee_id,
        email: u.email,
        jenis_cuti: leaveTypeName,
        tanggal_mulai: String(b.tanggal_mulai || '').trim(),
        tanggal_selesai: String(b.tanggal_selesai || '').trim(),
        jumlah_hari: days,
        alasan: String(b.alasan || '').trim(),
        lampiran_url: attachmentUrl,
        status: 'pending',
        approver_email: String(b.approver_email || '').trim(),
        created_at: nowIso(),
        updated_at: nowIso()
      };
      if (!payload.jenis_cuti || !payload.tanggal_mulai || !payload.tanggal_selesai) return json(res, 400, { ok: false, message: 'jenis_cuti, tanggal_mulai, tanggal_selesai wajib diisi.' });
      if (!/^\d{4}-\d{2}-\d{2}$/.test(payload.tanggal_mulai) || !/^\d{4}-\d{2}-\d{2}$/.test(payload.tanggal_selesai)) return json(res, 400, { ok: false, message: 'Format tanggal harus YYYY-MM-DD.' });
      if (payload.tanggal_mulai > payload.tanggal_selesai) return json(res, 400, { ok: false, message: 'Tanggal mulai tidak boleh lebih besar dari tanggal selesai.' });
      if (Number(payload.jumlah_hari || 0) <= 0) return json(res, 400, { ok: false, message: 'Rentang tanggal cuti tidak valid.' });
      const ins = await db('POST', 'leave_requests', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal submit leave.', error: ins.error });
      try {
        const admins = await getAdminEmails();
        const title = 'Pengajuan Cuti Baru Perlu Persetujuan';
        const requester = await getEmployeeDisplayName(u);
        const detailHtml = leaveMailTemplate(title, [
          'Halo Admin,',
          'Ada pengajuan cuti baru dari <b>' + String(requester || u.employee_id) + '</b>.',
          'Jenis cuti: <b>' + payload.jenis_cuti + '</b>',
          'Tanggal: <b>' + payload.tanggal_mulai + '</b> s/d <b>' + payload.tanggal_selesai + '</b>',
          'Silakan review di dashboard Leave Approval.'
        ]);
        for (const em of admins) {
          await sendEssEmail(em, title + ' - ESS', detailHtml);
        }
        await db('POST', 'announcements', null, {
          announcement_id: rid('ANN'),
          judul: 'Pengajuan Cuti Baru',
          isi: requester + ' mengajukan cuti ' + payload.jenis_cuti + ' (' + payload.tanggal_mulai + ' s/d ' + payload.tanggal_selesai + ').',
          target_role: 'admin',
          published_at: nowIso(),
          expired_at: null,
          is_active: true,
          created_by: u.email
        }, { Prefer: 'return=minimal' });
      } catch (_) {}
      return json(res, 200, { ok: true, message: 'Pengajuan cuti berhasil dikirim.', data: ins.data });
    }

    if (path === 'me/payroll-docs' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      let r = await db('GET', 'payroll_docs', { select: '*', employee_id: 'eq.' + u.employee_id, order: 'uploaded_at.desc', limit: Math.min(Number(req.query.limit || 24), 100) });
      if (r.ok && (!Array.isArray(r.data) || r.data.length === 0) && String(u.email || '').trim()) {
        r = await db('GET', 'payroll_docs', { select: '*', email: 'eq.' + String(u.email || '').trim().toLowerCase(), order: 'uploaded_at.desc', limit: Math.min(Number(req.query.limit || 24), 100) });
      }
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil payroll docs.', error: r.error });
      return json(res, 200, (r.data || []).map(enrichPayrollDoc));
    }

    if (path === 'me/payroll/summary' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      let rows = await db('GET', 'payroll_docs', { select: 'doc_id,bulan,tahun,nama_file,file_url,keterangan,uploaded_at,email,employee_id', employee_id: 'eq.' + u.employee_id, order: 'uploaded_at.desc', limit: 120 });
      if (rows.ok && (!Array.isArray(rows.data) || rows.data.length === 0) && String(u.email || '').trim()) {
        rows = await db('GET', 'payroll_docs', { select: 'doc_id,bulan,tahun,nama_file,file_url,keterangan,uploaded_at,email,employee_id', email: 'eq.' + String(u.email || '').trim().toLowerCase(), order: 'uploaded_at.desc', limit: 120 });
      }
      if (!rows.ok) return json(res, 500, { ok: false, message: 'Gagal ambil payroll summary.', error: rows.error });
      const data = (rows.data || []).map(enrichPayrollDoc);
      const latest = data[0] || null;
      const thisYear = String(new Date().getFullYear());
      const docsThisYear = data.filter(function(x) { return String(x.tahun || '') === thisYear; }).length;
      const periodMap = {};
      data.forEach(function(x) {
        const k = String(x.bulan || '-') + '-' + String(x.tahun || '-');
        periodMap[k] = true;
      });
      return json(res, 200, {
        ok: true,
        summary: {
          total_docs: data.length,
          docs_this_year: docsThisYear,
          unique_periods: Object.keys(periodMap).length,
          latest_uploaded_at: latest ? latest.uploaded_at : null,
          latest_take_home_pay: latest ? toMoney(latest.net_salary || latest.take_home_pay || 0) : 0,
          avg_take_home_pay: data.length > 0 ? toMoney(data.reduce(function(acc, x) { return acc + Number(x.net_salary || x.take_home_pay || 0); }, 0) / data.length) : 0
        },
        latest_doc: latest
      });
    }

    if (path === 'me/announcements' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const r = await db('GET', 'announcements', { select: '*', is_active: 'eq.true', or: '(target_role.eq.all,target_role.eq.' + u.role + ')', order: 'published_at.desc', limit: Math.min(Number(req.query.limit || 30), 100) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil announcements.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'me/notifications/summary' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const seen = await db('GET', 'notification_seen', { select: '*', email: 'eq.' + u.email, limit: 1 });
      if (!seen.ok) return json(res, 500, { ok: false, message: 'Gagal ambil notification seen.', error: seen.error });
      let row = (seen.data && seen.data[0]) || null;
      if (!row) {
        const up = await db('POST', 'notification_seen', { on_conflict: 'email' }, [{ email: u.email }], { Prefer: 'resolution=merge-duplicates,return=representation' });
        if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal init notification seen.', error: up.error });
        row = (up.data && up.data[0]) || { email: u.email };
      }
      const ann = await db('GET', 'announcements', { select: 'announcement_id,published_at', is_active: 'eq.true', or: '(target_role.eq.all,target_role.eq.' + u.role + ')' });
      const pay = await db('GET', 'payroll_docs', { select: 'doc_id,uploaded_at', employee_id: 'eq.' + u.employee_id });
      const leave = await db('GET', 'leave_requests', { select: 'leave_id,status,updated_at', employee_id: 'eq.' + u.employee_id, order: 'updated_at.desc', limit: 300 });
      if (!ann.ok || !pay.ok || !leave.ok) return json(res, 500, { ok: false, message: 'Gagal hitung notifikasi.', error: !ann.ok ? ann.error : (!pay.ok ? pay.error : leave.error) });
      const annTs = new Date(row.announcement_seen_at || 0).getTime() || 0;
      const payTs = new Date(row.payroll_seen_at || 0).getTime() || 0;
      const unreadA = (ann.data || []).filter(function(x) { return (new Date(x.published_at || 0).getTime() || 0) > annTs; }).length;
      const unreadP = (pay.data || []).filter(function(x) { return (new Date(x.uploaded_at || 0).getTime() || 0) > payTs; }).length;
      const leaveSeenTs = Math.max(annTs, payTs);
      const unreadL = (leave.data || []).filter(function(x) {
        const st = String(x.status || '').toLowerCase();
        if (st !== 'approved' && st !== 'rejected') return false;
        return (new Date(x.updated_at || 0).getTime() || 0) > leaveSeenTs;
      }).length;
      return json(res, 200, { unread_announcements: unreadA, unread_payroll_docs: unreadP, unread_leave_updates: unreadL, total_unread: unreadA + unreadP + unreadL });
    }

    if (path === 'me/operations-intelligence/summary' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const today = ymd();
      const startDate = dateShift(today, -13);
      const rCfg = await db('GET', 'config', { select: 'key,value', key: 'in.(LATE_AFTER_TIME,OPS_LATE_RATE_HIGH)', limit: 10 });
      const rAtt = await db('GET', 'attendance', {
        select: 'attendance_id,employee_id,tanggal,jam_masuk,jam_keluar,status',
        employee_id: 'eq.' + u.employee_id,
        and: '(tanggal.gte.' + startDate + ',tanggal.lte.' + today + ')',
        order: 'tanggal.asc,created_at.asc',
        limit: 2000
      });
      const rLeavePending = await db('GET', 'leave_requests', { select: 'leave_id,status', employee_id: 'eq.' + u.employee_id, status: 'eq.pending', limit: 500 });
      const rLeaveApproved = await db('GET', 'leave_requests', { select: 'leave_id,status,tanggal_mulai,tanggal_selesai,jenis_cuti', employee_id: 'eq.' + u.employee_id, status: 'eq.approved', limit: 500 });
      if (!rCfg.ok || !rAtt.ok || !rLeavePending.ok || !rLeaveApproved.ok) {
        const err = !rCfg.ok ? rCfg.error : !rAtt.ok ? rAtt.error : !rLeavePending.ok ? rLeavePending.error : rLeaveApproved.error;
        return json(res, 500, { ok: false, message: 'Gagal ambil employee intelligence.', error: err });
      }
      const cfgMap = {};
      (rCfg.data || []).forEach(function(x) { cfgMap[String(x.key || '')] = String(x.value || ''); });
      const lateAfter = cfgMap.LATE_AFTER_TIME || '08:30:00';
      const lateHigh = Number(cfgMap.OPS_LATE_RATE_HIGH || '20');
      const rows = rAtt.data || [];
      const presentCount = rows.length;
      const lateCount = rows.filter(function(r) {
        const st = String(r.status || '').toLowerCase();
        const jm = String(r.jam_masuk || '');
        return st === 'terlambat' || (jm && jm > lateAfter);
      }).length;
      const ontimeCount = Math.max(0, presentCount - lateCount);
      const lateRate = presentCount > 0 ? Math.round((lateCount / presentCount) * 10000) / 100 : 0;
      const todayRow = rows.filter(function(r) { return String(r.tanggal || '') === today; }).slice(-1)[0] || null;
      const pendingLeaves = Array.isArray(rLeavePending.data) ? rLeavePending.data.length : 0;
      const approvedLeaveToday = (rLeaveApproved.data || []).find(function(r) {
        const s = String(r.tanggal_mulai || '');
        const e = String(r.tanggal_selesai || '');
        return s && e && s <= today && e >= today;
      }) || null;
      const alerts = [];
      if (approvedLeaveToday) alerts.push({ severity: 'info', code: 'ON_LEAVE', title: 'Status Hari Ini: Cuti', detail: 'Kamu sedang cuti (' + String(approvedLeaveToday.jenis_cuti || '-') + ').' });
      else if (!todayRow) alerts.push({ severity: 'high', code: 'NO_CHECKIN', title: 'Belum Check-in Hari Ini', detail: 'Silakan lakukan check-in agar jam kerja mulai tercatat.' });
      else if (!todayRow.jam_keluar) alerts.push({ severity: 'medium', code: 'CHECKOUT_PENDING', title: 'Belum Check-out', detail: 'Jangan lupa check-out saat jam kerja selesai.' });
      if (lateRate >= lateHigh) alerts.push({ severity: 'high', code: 'LATE_TREND', title: 'Tren Keterlambatan Tinggi', detail: 'Late rate 14 hari terakhir: ' + lateRate + '%.' });
      if (pendingLeaves >= 2) alerts.push({ severity: 'medium', code: 'LEAVE_PENDING', title: 'Pengajuan Cuti Masih Pending', detail: pendingLeaves + ' pengajuan cuti belum diproses admin.' });
      if (!alerts.length) alerts.push({ severity: 'info', code: 'STABLE', title: 'Kondisi Kehadiran Stabil', detail: 'Pertahankan ritme kehadiran dan performa kerja harian.' });
      const recommendations = [];
      if (!todayRow && !approvedLeaveToday) recommendations.push({ priority: 1, text: 'Lakukan check-in sekarang untuk memulai perhitungan jam kerja.' });
      if (todayRow && !todayRow.jam_keluar) recommendations.push({ priority: 2, text: 'Pastikan check-out tepat waktu agar data absensi lengkap.' });
      if (lateRate >= lateHigh) recommendations.push({ priority: 3, text: 'Atur pengingat datang lebih awal 15-30 menit dari batas keterlambatan.' });
      if (pendingLeaves >= 2) recommendations.push({ priority: 4, text: 'Cek menu leave untuk memastikan dokumen pendukung sudah lengkap.' });
      if (!recommendations.length) recommendations.push({ priority: 1, text: 'Kehadiran bagus. Pertahankan konsistensi dan disiplin waktu.' });
      recommendations.sort(function(a1, b1) { return Number(a1.priority || 99) - Number(b1.priority || 99); });
      const quickActions = [];
      if (!todayRow && !approvedLeaveToday) quickActions.push({ action_key: 'checkin_now', label: 'Check In Sekarang' });
      if (todayRow && !todayRow.jam_keluar) quickActions.push({ action_key: 'checkout_or_break', label: 'Kelola Break / Check Out' });
      if (pendingLeaves > 0) quickActions.push({ action_key: 'open_leave_status', label: 'Lihat Status Cuti' });
      if (!quickActions.length) quickActions.push({ action_key: 'open_attendance_history', label: 'Lihat Riwayat Absensi' });
      return json(res, 200, {
        ok: true,
        generated_at: nowIso(),
        period: { start_date: startDate, end_date: today },
        summary: {
          attendance_records_period: presentCount,
          late_records_period: lateCount,
          ontime_records_period: ontimeCount,
          late_rate_period: lateRate,
          pending_leaves: pendingLeaves,
          checked_in_today: !!(todayRow && todayRow.jam_masuk),
          checked_out_today: !!(todayRow && todayRow.jam_keluar),
          on_leave_today: !!approvedLeaveToday
        },
        alerts: alerts,
        recommendations: recommendations,
        quick_actions: quickActions
      });
    }

    if (path === 'me/operations-intelligence/reminder-plan' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const today = ymd();
      const [cfg, todayAtt, pendingLeaves] = await Promise.all([
        db('GET', 'config', { select: 'key,value', key: 'in.(WORK_START_TIME,LATE_AFTER_TIME)', limit: 10 }),
        db('GET', 'attendance', { select: 'jam_masuk,jam_keluar,break_active,status', employee_id: 'eq.' + u.employee_id, tanggal: 'eq.' + today, order: 'created_at.desc', limit: 1 }),
        db('GET', 'leave_requests', { select: 'leave_id', employee_id: 'eq.' + u.employee_id, status: 'eq.pending', limit: 500 })
      ]);
      if (!cfg.ok || !todayAtt.ok || !pendingLeaves.ok) {
        const err = !cfg.ok ? cfg.error : !todayAtt.ok ? todayAtt.error : pendingLeaves.error;
        return json(res, 500, { ok: false, message: 'Gagal ambil reminder plan.', error: err });
      }
      const map = {};
      (cfg.data || []).forEach(function(x) { map[String(x.key || '')] = String(x.value || ''); });
      const workStart = map.WORK_START_TIME || '08:00:00';
      const lateAfter = map.LATE_AFTER_TIME || '08:30:00';
      const row = (todayAtt.data && todayAtt.data[0]) || null;
      const pendingCount = Array.isArray(pendingLeaves.data) ? pendingLeaves.data.length : 0;
      let reminder = { action_key: 'open_attendance_history', label: 'Cek Riwayat Kehadiran', urgency: 'low', detail: 'Kondisi hari ini stabil.' };
      if (!row || !row.jam_masuk) reminder = { action_key: 'checkin_now', label: 'Check In Sekarang', urgency: 'high', detail: 'Belum ada check-in. Batas terlambat: ' + lateAfter + '.' };
      else if (!row.jam_keluar && String(row.break_active || '').toLowerCase() === 'true') reminder = { action_key: 'checkout_or_break', label: 'Akhiri Break / Lanjut Kerja', urgency: 'medium', detail: 'Break masih aktif. Pastikan durasi break terkontrol.' };
      else if (!row.jam_keluar) reminder = { action_key: 'checkout_or_break', label: 'Ingat Check Out', urgency: 'medium', detail: 'Setelah jam kerja selesai, lakukan check-out.' };
      if (pendingCount > 0) reminder = { action_key: 'open_leave_status', label: 'Review Pengajuan Cuti', urgency: 'medium', detail: pendingCount + ' pengajuan cuti masih pending.' };
      return json(res, 200, { ok: true, date: today, work_start_time: workStart, late_after_time: lateAfter, pending_leaves: pendingCount, reminder: reminder });
    }

    if (path === 'me/notifications/mark-seen' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      const b = await readBody(req);
      const type = String(b.type || 'all').toLowerCase();
      const payload = { email: u.email, updated_at: nowIso() };
      if (type === 'all' || type === 'announcement') payload.announcement_seen_at = nowIso();
      if (type === 'all' || type === 'payroll') payload.payroll_seen_at = nowIso();
      const up = await db('POST', 'notification_seen', { on_conflict: 'email' }, [payload], { Prefer: 'resolution=merge-duplicates,return=representation' });
      if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal mark seen.', error: up.error });
      return json(res, 200, { ok: true, message: 'Notifikasi ditandai dilihat.' });
    }

    if (path === 'me/notifications' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const seen = await db('GET', 'notification_seen', { select: '*', email: 'eq.' + u.email, limit: 1 });
      const row = seen.ok && seen.data && seen.data[0] ? seen.data[0] : { announcement_seen_at: null, payroll_seen_at: null };
      const ann = await db('GET', 'announcements', { select: 'announcement_id,judul,isi,published_at,target_role,is_active', is_active: 'eq.true', or: '(target_role.eq.all,target_role.eq.' + u.role + ')' });
      const pay = await db('GET', 'payroll_docs', { select: 'doc_id,nama_file,bulan,tahun,file_url,uploaded_at', employee_id: 'eq.' + u.employee_id });
      const leave = await db('GET', 'leave_requests', { select: 'leave_id,status,jenis_cuti,tanggal_mulai,tanggal_selesai,updated_at', employee_id: 'eq.' + u.employee_id, order: 'updated_at.desc', limit: 200 });
      if (!ann.ok || !pay.ok || !leave.ok) return json(res, 500, { ok: false, message: 'Gagal ambil notifikasi.', error: !ann.ok ? ann.error : (!pay.ok ? pay.error : leave.error) });
      const annTs = new Date(row.announcement_seen_at || 0).getTime() || 0;
      const payTs = new Date(row.payroll_seen_at || 0).getTime() || 0;
      const notifications = [];
      (ann.data || []).forEach(function(x) {
        const ts = new Date(x.published_at || 0).getTime() || 0;
        notifications.push({ notification_type: 'announcement', item_id: x.announcement_id || '', title: x.judul || 'Pengumuman Baru', message: x.isi || '', date_value: x.published_at || '', is_unread: ts > annTs, action_label: 'Lihat Pengumuman' });
      });
      (pay.data || []).forEach(function(x) {
        const ts = new Date(x.uploaded_at || 0).getTime() || 0;
        notifications.push({ notification_type: 'payroll', item_id: x.doc_id || '', title: 'Slip Gaji Baru Tersedia', message: (x.nama_file || 'Dokumen payroll') + ' • ' + (x.bulan || '-') + ' ' + (x.tahun || '-'), date_value: x.uploaded_at || '', is_unread: ts > payTs, file_url: x.file_url || '', action_label: 'Buka Slip Gaji' });
      });
      const leaveSeenTs = Math.max(annTs, payTs);
      (leave.data || []).forEach(function(x) {
        const st = String(x.status || '').toLowerCase();
        if (st !== 'approved' && st !== 'rejected') return;
        const ts = new Date(x.updated_at || 0).getTime() || 0;
        notifications.push({
          notification_type: 'leave',
          item_id: x.leave_id || '',
          title: st === 'approved' ? 'Pengajuan Cuti Disetujui' : 'Pengajuan Cuti Ditolak',
          message: String(x.jenis_cuti || '-') + ' (' + String(x.tanggal_mulai || '-') + ' s/d ' + String(x.tanggal_selesai || '-') + ')',
          date_value: x.updated_at || '',
          is_unread: ts > leaveSeenTs,
          action_label: 'Lihat Cuti'
        });
      });
      notifications.sort(function(a, b) { return new Date(b.date_value || 0).getTime() - new Date(a.date_value || 0).getTime(); });
      return json(res, 200, notifications);
    }

    if (path === 'me/incidents/timeline' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const limit = Math.min(Number(req.query.limit || 120), 300);
      const [att, leaves, ann] = await Promise.all([
        db('GET', 'attendance', { select: 'attendance_id,tanggal,jam_masuk,jam_keluar,status,created_at', employee_id: 'eq.' + u.employee_id, order: 'tanggal.desc,created_at.desc', limit: 120 }),
        db('GET', 'leave_requests', { select: 'leave_id,status,jenis_cuti,tanggal_mulai,tanggal_selesai,created_at,updated_at', employee_id: 'eq.' + u.employee_id, order: 'created_at.desc', limit: 120 }),
        db('GET', 'announcements', { select: 'announcement_id,judul,isi,published_at,target_role,is_active', is_active: 'eq.true', or: '(target_role.eq.all,target_role.eq.' + u.role + ')', order: 'published_at.desc', limit: 60 })
      ]);
      if (!att.ok || !leaves.ok || !ann.ok) return json(res, 500, { ok: false, message: 'Gagal ambil timeline personal.', error: (!att.ok ? att.error : (!leaves.ok ? leaves.error : ann.error)) });
      const timeline = [];
      (att.data || []).forEach(function(x) {
        const st = String(x.status || '').toLowerCase();
        timeline.push({
          category: 'attendance',
          severity: st === 'terlambat' ? 'high' : 'info',
          title: st === 'terlambat' ? 'Kehadiran Terlambat' : 'Kehadiran Tercatat',
          detail: 'Tanggal ' + String(x.tanggal || '-') + ' • masuk ' + String(x.jam_masuk || '-') + ' • keluar ' + String(x.jam_keluar || '-'),
          occurred_at: x.created_at || x.tanggal || '',
          action_route: 'history'
        });
      });
      (leaves.data || []).forEach(function(x) {
        const st = String(x.status || '').toLowerCase();
        timeline.push({
          category: 'leave',
          severity: st === 'pending' ? 'medium' : (st === 'rejected' ? 'high' : 'info'),
          title: 'Status Cuti: ' + String(x.status || '-'),
          detail: String(x.jenis_cuti || '-') + ' (' + String(x.tanggal_mulai || '-') + ' s/d ' + String(x.tanggal_selesai || '-') + ')',
          occurred_at: x.updated_at || x.created_at || '',
          action_route: 'leave'
        });
      });
      (ann.data || []).forEach(function(x) {
        timeline.push({
          category: 'announcement',
          severity: 'info',
          title: String(x.judul || 'Pengumuman'),
          detail: String(x.isi || '').slice(0, 180),
          occurred_at: x.published_at || '',
          action_route: 'notif'
        });
      });
      timeline.sort(function(a1, b1) { return new Date(b1.occurred_at || 0).getTime() - new Date(a1.occurred_at || 0).getTime(); });
      return json(res, 200, { ok: true, timeline: timeline.slice(0, limit) });
    }

    if (path === 'admin/employees' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await db('GET', 'employees', { select: '*', order: 'created_at.desc', limit: Math.min(Number(req.query.limit || 200), 1000) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil employees.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'admin/employees' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const payload = employeePayloadFromInput(b);
      const created = await createEmployeeWithActivation(payload);
      if (!created.ok) return json(res, Number(created.status || 500), { ok: false, message: created.message || 'Gagal tambah employee.', error: created.error });
      await auditLog(a.email, 'CREATE', 'employees', 'Tambah employee ' + payload.employee_id + ' (' + payload.email + ')', String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: created.message, activation_password: created.activation_password, activation_delivery: created.activation_delivery, data: created.data });
    }

    if (path === 'admin/employees/import-csv' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      let csvText = String(b.csv_text || '').trim();
      if (!csvText && b.csv_base64) {
        try { csvText = Buffer.from(String(b.csv_base64 || ''), 'base64').toString('utf8'); } catch (_) { csvText = ''; }
      }
      if (!csvText) return json(res, 400, { ok: false, message: 'File CSV kosong.' });
      const rows = csvRowsToObjects(csvText);
      if (!rows.length) return json(res, 400, { ok: false, message: 'Tidak ada baris data pada CSV.' });
      const maxRows = Math.min(Number(b.max_rows || 1000), 1000);
      const selected = rows.slice(0, maxRows);
      const createdRows = [];
      const failedRows = [];
      for (let i = 0; i < selected.length; i += 1) {
        const row = selected[i] || {};
        const payload = employeePayloadFromInput(row);
        const created = await createEmployeeWithActivation(payload, { skipDivisionValidation: true });
        if (created.ok) {
          createdRows.push({
            row_number: i + 2,
            employee_id: payload.employee_id,
            email: payload.email,
            activation_password: created.activation_password,
            activation_delivery: created.activation_delivery
          });
        } else {
          failedRows.push({
            row_number: i + 2,
            employee_id: payload.employee_id || '',
            email: payload.email || '',
            message: normalizeApiErrorMessage(created.message || created.error, 'Gagal tambah employee.')
          });
        }
      }
      await auditLog(a.email, 'CREATE', 'employees', 'Import CSV employees total=' + String(selected.length) + ', sukses=' + String(createdRows.length) + ', gagal=' + String(failedRows.length), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, {
        ok: true,
        message: 'Import CSV selesai.',
        total_rows: selected.length,
        created_count: createdRows.length,
        failed_count: failedRows.length,
        created: createdRows,
        failed: failedRows
      });
    }

    if (path === 'admin/auth/activation/reset' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const employeeId = String(b.employee_id || '').trim();
      if (!employeeId) return json(res, 400, { ok: false, message: 'employee_id wajib diisi.' });
      const e = await db('GET', 'employees', { select: 'employee_id,email,nama,role', employee_id: 'eq.' + employeeId, limit: 1 });
      if (!e.ok) return json(res, 500, { ok: false, message: 'Gagal ambil employee.', error: e.error });
      const row = Array.isArray(e.data) && e.data[0] ? e.data[0] : null;
      if (!row) return json(res, 404, { ok: false, message: 'Employee tidak ditemukan.' });
      const actorEmp = String(a.employee_id || '').trim();
      const actorMail = String(a.email || '').trim().toLowerCase();
      if ((actorEmp && actorEmp === employeeId) || (actorMail && actorMail === String(row.email || '').trim().toLowerCase())) return json(res, 403, { ok: false, message: 'Tidak dapat reset password akun yang sedang digunakan login.' });
      const rl = await consumeResetRateLimit(employeeId, a.email);
      if (!rl.ok) return json(res, 429, { ok: false, message: rl.message, retry_seconds: rl.retry_seconds });
      const requestedPassword = String(b.new_password || '').trim();
      if (requestedPassword && requestedPassword.length < 8) return json(res, 400, { ok: false, message: 'Password minimal 8 karakter.' });
      const activationPassword = requestedPassword || randomPassword(10);
      await upsertAuthCred(String(row.email || ''), {
        employee_id: String(row.employee_id || ''),
        password_hash: hashSha256(activationPassword),
        must_change_password: true,
        first_login_required: true,
        activation_sent_at: nowIso(),
        password_last_set_at: nowIso()
      });
      const delivery = await deliverActivationEmail(String(row.email || ''), String(row.nama || ''), activationPassword, { username: String(row.email || ''), mode: requestedPassword ? 'set' : 'reset', role: String(row.role || '') });
      await recordActivationDelivery(employeeId, String(row.email || ''), activationPassword, delivery, requestedPassword ? 'set' : 'reset', a.email);
      await auditLog(a.email, 'UPDATE', 'auth', 'Reset password aktivasi ' + employeeId, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: delivery.sent ? 'Password aktivasi baru terkirim ke email.' : 'Password aktivasi baru dibuat. Kirim manual jika email belum aktif.', employee_id: employeeId, email: row.email, role: row.role, activation_password: activationPassword, activation_delivery: delivery });
    }

    if (path === 'admin/auth/activation/resend' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const employeeId = String(b.employee_id || '').trim();
      if (!employeeId) return json(res, 400, { ok: false, message: 'employee_id wajib diisi.' });
      const e = await db('GET', 'employees', { select: 'employee_id,email,nama,role', employee_id: 'eq.' + employeeId, limit: 1 });
      if (!e.ok) return json(res, 500, { ok: false, message: 'Gagal ambil employee.', error: e.error });
      const row = Array.isArray(e.data) && e.data[0] ? e.data[0] : null;
      if (!row) return json(res, 404, { ok: false, message: 'Employee tidak ditemukan.' });
      const out = await db('GET', 'config', { select: 'value', key: 'eq.' + 'AUTH_ACTIVATION_OUTBOX_' + employeeId, limit: 1 });
      const outVal = out.ok && Array.isArray(out.data) && out.data[0] ? safeJsonParse(out.data[0].value, {}) : {};
      const lastPassword = String(outVal.activation_password || '').trim();
      if (!lastPassword) return json(res, 400, { ok: false, message: 'Tidak ada password aktivasi terakhir. Gunakan Reset Password terlebih dahulu.' });
      const delivery = await deliverActivationEmail(String(row.email || ''), String(row.nama || ''), lastPassword, { username: String(row.email || ''), mode: 'resend', role: String(row.role || '') });
      await recordActivationDelivery(employeeId, String(row.email || ''), lastPassword, delivery, 'resend', a.email);
      await auditLog(a.email, 'UPDATE', 'auth', 'Resend activation email ' + employeeId, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: delivery.sent ? 'Email aktivasi berhasil dikirim ulang.' : 'Gagal kirim ulang email aktivasi.', employee_id: employeeId, email: row.email, activation_delivery: delivery });
    }

    if (path === 'admin/auth/activation/batch-resend' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const ids = Array.isArray(b.employee_ids) ? b.employee_ids : [];
      const uniqueIds = Array.from(new Set(ids.map(function(x) { return String(x || '').trim(); }).filter(Boolean))).slice(0, 100);
      if (!uniqueIds.length) return json(res, 400, { ok: false, message: 'employee_ids wajib diisi.' });
      const sent = [];
      const failed = [];
      for (let i = 0; i < uniqueIds.length; i += 1) {
        const employeeId = uniqueIds[i];
        const e = await db('GET', 'employees', { select: 'employee_id,email,nama,role', employee_id: 'eq.' + employeeId, limit: 1 });
        const row = e.ok && Array.isArray(e.data) && e.data[0] ? e.data[0] : null;
        if (!row) { failed.push({ employee_id: employeeId, message: 'Employee tidak ditemukan.' }); continue; }
        const out = await db('GET', 'config', { select: 'value', key: 'eq.' + 'AUTH_ACTIVATION_OUTBOX_' + employeeId, limit: 1 });
        const outVal = out.ok && Array.isArray(out.data) && out.data[0] ? safeJsonParse(out.data[0].value, {}) : {};
        const lastPassword = String(outVal.activation_password || '').trim();
        if (!lastPassword) { failed.push({ employee_id: employeeId, message: 'Tidak ada password aktivasi terakhir.' }); continue; }
        const delivery = await deliverActivationEmail(String(row.email || ''), String(row.nama || ''), lastPassword, { username: String(row.email || ''), mode: 'resend', role: String(row.role || '') });
        await recordActivationDelivery(employeeId, String(row.email || ''), lastPassword, delivery, 'resend', a.email);
        if (delivery.sent) sent.push({ employee_id: employeeId, email: String(row.email || ''), provider_id: String(delivery.provider_id || '') });
        else failed.push({ employee_id: employeeId, email: String(row.email || ''), message: String(delivery.error || delivery.message || 'Gagal kirim') });
      }
      await auditLog(a.email, 'UPDATE', 'auth', 'Batch resend activation total=' + String(uniqueIds.length) + ', sent=' + String(sent.length) + ', failed=' + String(failed.length), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Batch resend selesai.', total: uniqueIds.length, sent_count: sent.length, failed_count: failed.length, sent: sent, failed: failed });
    }

    if (path === 'admin/auth/activation/audit' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const employeeId = String((req.query && req.query.employee_id) || '').trim();
      if (!employeeId) return json(res, 400, { ok: false, message: 'employee_id wajib diisi.' });
      const limit = Math.max(1, Math.min(30, Number((req.query && req.query.limit) || 10)));
      const prefix = 'AUTH_EMAIL_AUDIT_' + employeeId + '_';
      const q = await db('GET', 'config', { select: 'key,value', key: 'like.' + prefix + '%', limit: String(limit) });
      if (!q.ok) return json(res, 500, { ok: false, message: 'Gagal mengambil audit email.', error: q.error });
      const rows = (Array.isArray(q.data) ? q.data : []).map(function(r) {
        const v = safeJsonParse(r.value, {});
        return {
          key: String(r.key || ''),
          created_at: String(v.created_at || ''),
          mode: String(v.mode || ''),
          sent: !!v.sent,
          sender: String(v.sender || ''),
          to: String(v.to || ''),
          provider_id: String(v.provider_id || ''),
          warning: String(v.warning || ''),
          error: String(v.error || '')
        };
      }).sort(function(a1, a2) { return String(a2.created_at || a2.key || '').localeCompare(String(a1.created_at || a1.key || '')); }).slice(0, limit);
      return json(res, 200, { ok: true, employee_id: employeeId, total: rows.length, rows: rows });
    }

    if (path === 'admin/auth/activation/deliverability' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const limit = Math.max(20, Math.min(1000, Number((req.query && req.query.limit) || 200)));
      const startDate = String((req.query && req.query.start_date) || '').trim();
      const endDate = String((req.query && req.query.end_date) || '').trim();
      const modeFilter = String((req.query && req.query.mode) || '').trim().toLowerCase();
      if (startDate && !isValidDateYmd(startDate)) return json(res, 400, { ok: false, message: 'start_date harus format YYYY-MM-DD.' });
      if (endDate && !isValidDateYmd(endDate)) return json(res, 400, { ok: false, message: 'end_date harus format YYYY-MM-DD.' });
      const q = await db('GET', 'config', { select: 'key,value', key: 'like.AUTH_EMAIL_AUDIT_%', limit: String(limit) });
      if (!q.ok) return json(res, 500, { ok: false, message: 'Gagal mengambil data deliverability.', error: q.error });
      const startTs = startDate ? Date.parse(startDate + 'T00:00:00.000Z') : 0;
      const endTs = endDate ? Date.parse(endDate + 'T23:59:59.999Z') : 0;
      const rows = (Array.isArray(q.data) ? q.data : []).map(function(r) {
        const v = safeJsonParse(r.value, {});
        return {
          key: String(r.key || ''),
          employee_id: (String(r.key || '').split('_')[3] || ''),
          created_at: String(v.created_at || ''),
          mode: String(v.mode || ''),
          sent: !!v.sent,
          to: String(v.to || ''),
          sender: String(v.sender || ''),
          error: String(v.error || '')
        };
      }).filter(function(r) {
        if (modeFilter && String(r.mode || '').toLowerCase() !== modeFilter) return false;
        const ts = Date.parse(String(r.created_at || ''));
        if (startTs && (!ts || ts < startTs)) return false;
        if (endTs && (!ts || ts > endTs)) return false;
        return true;
      }).sort(function(a1, a2) { return String(a2.created_at || a2.key || '').localeCompare(String(a1.created_at || a1.key || '')); });
      const sentCount = rows.filter(function(x) { return x.sent; }).length;
      const failCount = rows.length - sentCount;
      const successRate = rows.length ? Math.round((sentCount / rows.length) * 1000) / 10 : 0;
      const modeStats = {};
      rows.forEach(function(r) {
        const m = r.mode || 'unknown';
        if (!modeStats[m]) modeStats[m] = { total: 0, sent: 0, failed: 0 };
        modeStats[m].total += 1;
        if (r.sent) modeStats[m].sent += 1; else modeStats[m].failed += 1;
      });
      const recentErrors = rows.filter(function(x) { return !x.sent; }).slice(0, 10).map(function(x) {
        return { created_at: x.created_at, employee_id: x.employee_id, to: x.to, mode: x.mode, error: x.error };
      });
      return json(res, 200, { ok: true, total: rows.length, sent: sentCount, failed: failCount, success_rate: successRate, mode_stats: modeStats, recent_errors: recentErrors, rows: rows.slice(0, 200) });
    }

    if (path === 'admin/employees' && method === 'PATCH') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const employeeId = String(b.employee_id || '').trim();
      if (!employeeId) return json(res, 400, { ok: false, message: 'employee_id wajib diisi.' });
      const cur = await db('GET', 'employees', { select: '*', employee_id: 'eq.' + employeeId, limit: 1 });
      if (!cur.ok) return json(res, 500, { ok: false, message: 'Gagal validasi employee.', error: cur.error });
      const current = Array.isArray(cur.data) && cur.data[0] ? cur.data[0] : null;
      if (!current) return json(res, 404, { ok: false, message: 'Employee tidak ditemukan.' });
      const patch = { updated_at: nowIso() };
      if (b.email !== undefined) patch.email = String(b.email || '').trim().toLowerCase();
      if (b.nama !== undefined) patch.nama = String(b.nama || '').trim();
      if (b.nik !== undefined) patch.nik = String(b.nik || '').trim();
      if (b.divisi !== undefined) patch.divisi = String(b.divisi || '').trim();
      if (b.jabatan !== undefined) patch.jabatan = String(b.jabatan || '').trim();
      if (b.atasan_email !== undefined) patch.atasan_email = String(b.atasan_email || '').trim().toLowerCase();
      if (b.status_karyawan !== undefined) patch.status_karyawan = String(b.status_karyawan || '').trim();
      if (b.tanggal_masuk !== undefined) patch.tanggal_masuk = b.tanggal_masuk || null;
      if (b.jatah_cuti !== undefined) patch.jatah_cuti = Number(b.jatah_cuti || 0);
      if (b.sisa_cuti !== undefined) patch.sisa_cuti = Number(b.sisa_cuti || 0);
      if (b.role !== undefined) patch.role = String(b.role || '').trim().toLowerCase();
      if (b.is_active !== undefined) {
        if (!isBooleanLike(b.is_active)) return json(res, 400, { ok: false, message: 'is_active harus boolean.' });
        patch.is_active = String(b.is_active).toLowerCase() === 'true';
      }
      if (b.no_hp !== undefined) patch.no_hp = String(b.no_hp || '').trim();
      if (b.alamat !== undefined) patch.alamat = String(b.alamat || '').trim();
      if (b.tempat_lahir !== undefined) patch.tempat_lahir = String(b.tempat_lahir || '').trim();
      if (b.tanggal_lahir !== undefined) patch.tanggal_lahir = b.tanggal_lahir || null;
      const allowedRoles = ['employee', 'admin', 'superadmin', 'manager'];
      const allowedStatus = ['Tetap', 'Kontrak', 'Magang', 'Probation', 'Outsource'];
      if (patch.email !== undefined && patch.email && !isValidEmail(patch.email)) return json(res, 400, { ok: false, message: 'Format email tidak valid.' });
      if (patch.role !== undefined && !allowedRoles.includes(patch.role)) return json(res, 400, { ok: false, message: 'Role tidak valid.' });
      if (patch.status_karyawan !== undefined && !allowedStatus.includes(patch.status_karyawan)) return json(res, 400, { ok: false, message: 'Status karyawan tidak valid.' });
      if (patch.tanggal_masuk !== undefined && patch.tanggal_masuk !== null && !isValidDateYmd(patch.tanggal_masuk)) return json(res, 400, { ok: false, message: 'tanggal_masuk harus format YYYY-MM-DD.' });
      if (patch.tanggal_lahir !== undefined && patch.tanggal_lahir !== null && !isValidDateYmd(patch.tanggal_lahir)) return json(res, 400, { ok: false, message: 'tanggal_lahir harus format YYYY-MM-DD.' });
      if (patch.jatah_cuti !== undefined && Number(patch.jatah_cuti) < 0) return json(res, 400, { ok: false, message: 'Jatah cuti tidak boleh negatif.' });
      if (patch.sisa_cuti !== undefined && Number(patch.sisa_cuti) < 0) return json(res, 400, { ok: false, message: 'Sisa cuti tidak boleh negatif.' });
      const finalJatah = patch.jatah_cuti !== undefined ? Number(patch.jatah_cuti) : Number(current.jatah_cuti || 0);
      const finalSisa = patch.sisa_cuti !== undefined ? Number(patch.sisa_cuti) : Number(current.sisa_cuti || 0);
      if (finalSisa > finalJatah) return json(res, 400, { ok: false, message: 'Sisa cuti tidak boleh lebih besar dari jatah cuti.' });
      if (patch.divisi !== undefined || patch.jabatan !== undefined) {
        const finalDivisi = patch.divisi !== undefined ? patch.divisi : String(current.divisi || '');
        const finalJabatan = patch.jabatan !== undefined ? patch.jabatan : String(current.jabatan || '');
        const vdp = await validateDivisionAndPosition(finalDivisi, finalJabatan);
        if (!vdp.ok) return json(res, 400, { ok: false, message: vdp.message, error: vdp.error });
      }
      if (Object.keys(patch).length <= 1) return json(res, 400, { ok: false, message: 'Tidak ada field yang diupdate.' });
      const upd = await db('PATCH', 'employees', { employee_id: 'eq.' + employeeId }, patch, { Prefer: 'return=representation' });
      if (!upd.ok) return json(res, 500, { ok: false, message: 'Gagal update employee.', error: upd.error });
      if (!Array.isArray(upd.data) || upd.data.length === 0) return json(res, 404, { ok: false, message: 'Employee tidak ditemukan.' });
      await auditLog(a.email, 'UPDATE', 'employees', 'Update employee ' + employeeId, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Employee berhasil diperbarui.', data: upd.data });
    }

    if (path === 'admin/employees' && method === 'DELETE') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const employeeId = String(b.employee_id || '').trim();
      const del = await deleteEmployeeCompletely(employeeId, a.email, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''), a.employee_id);
      if (!del.ok) return json(res, Number(del.status || 500), { ok: false, message: del.message || 'Gagal hapus employee.', error: del.error });
      return json(res, 200, { ok: true, message: del.message, employee_id: del.employee_id, email: del.email, nama: del.nama });
    }

    if (path === 'admin/employees/batch-delete' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const ids = Array.isArray(b.employee_ids) ? b.employee_ids : [];
      const uniqueIds = Array.from(new Set(ids.map(function(x) { return String(x || '').trim(); }).filter(Boolean))).slice(0, 300);
      if (!uniqueIds.length) return json(res, 400, { ok: false, message: 'employee_ids wajib diisi.' });
      const deleted = [];
      const failed = [];
      for (let i = 0; i < uniqueIds.length; i += 1) {
        const id = uniqueIds[i];
        const del = await deleteEmployeeCompletely(id, a.email, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''), a.employee_id);
        if (del.ok) deleted.push({ employee_id: id, email: del.email || '', nama: del.nama || '' });
        else failed.push({ employee_id: id, message: del.message || 'Gagal hapus employee.' });
      }
      return json(res, 200, { ok: true, message: 'Batch delete selesai.', total: uniqueIds.length, deleted_count: deleted.length, failed_count: failed.length, deleted: deleted, failed: failed });
    }

    if (path === 'admin/employees/detail' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const employeeId = String(req.query.employee_id || '').trim();
      if (!employeeId) return json(res, 400, { ok: false, message: 'employee_id wajib diisi.' });
      const limit = Math.min(Number(req.query.limit || 10), 50);
      const emp = await db('GET', 'employees', { select: '*', employee_id: 'eq.' + employeeId, limit: 1 });
      if (!emp.ok) return json(res, 500, { ok: false, message: 'Gagal ambil detail employee.', error: emp.error });
      const employee = Array.isArray(emp.data) && emp.data[0] ? emp.data[0] : null;
      if (!employee) return json(res, 404, { ok: false, message: 'Employee tidak ditemukan.' });
      const attendance = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + employeeId, order: 'tanggal.desc,created_at.desc', limit: limit });
      const leaves = await db('GET', 'leave_requests', { select: '*', employee_id: 'eq.' + employeeId, order: 'created_at.desc', limit: limit });
      const payroll = await db('GET', 'payroll_docs', { select: '*', employee_id: 'eq.' + employeeId, order: 'uploaded_at.desc', limit: limit });
      const email = String(employee.email || '').trim().toLowerCase();
      const auditByUser = email ? await db('GET', 'audit_log', { select: '*', user_email: 'eq.' + email, order: 'timestamp.desc', limit: limit }) : { ok: true, data: [] };
      const auditByDetail = await db('GET', 'audit_log', { select: '*', detail: 'ilike.*' + employeeId + '*', order: 'timestamp.desc', limit: limit });
      if (!attendance.ok || !leaves.ok || !payroll.ok || !auditByUser.ok || !auditByDetail.ok) {
        const err = !attendance.ok ? attendance.error : !leaves.ok ? leaves.error : !payroll.ok ? payroll.error : !auditByUser.ok ? auditByUser.error : auditByDetail.error;
        return json(res, 500, { ok: false, message: 'Gagal ambil detail riwayat employee.', error: err });
      }
      const recentAttendance = attendance.data || [];
      const recentLeaves = leaves.data || [];
      const recentPayroll = payroll.data || [];
      const auditMap = {};
      (auditByUser.data || []).concat(auditByDetail.data || []).forEach(function(x) {
        const key = String(x.log_id || '');
        if (!key) return;
        if (!auditMap[key]) auditMap[key] = x;
      });
      const recentAudit = Object.values(auditMap).sort(function(x, y) {
        return new Date(y.timestamp || 0).getTime() - new Date(x.timestamp || 0).getTime();
      }).slice(0, limit);
      const summary = {
        attendance_total: recentAttendance.length,
        leave_total: recentLeaves.length,
        pending_leaves: recentLeaves.filter(function(x) { return String(x.status || '').toLowerCase() === 'pending'; }).length,
        payroll_total: recentPayroll.length,
        last_attendance_date: recentAttendance[0] ? (recentAttendance[0].tanggal || '') : '',
        last_leave_date: recentLeaves[0] ? (recentLeaves[0].created_at || '') : '',
        last_payroll_at: recentPayroll[0] ? (recentPayroll[0].uploaded_at || '') : ''
      };
      return json(res, 200, {
        ok: true,
        employee: employee,
        summary: summary,
        recent_attendance: recentAttendance,
        recent_leaves: recentLeaves,
        recent_payroll: recentPayroll,
        recent_audit: recentAudit
      });
    }

    if (path === 'admin/schedules/monthly' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      await syncDivisionPositionFromEmployees(a.email);
      const month = String(req.query.month || ymd().slice(0, 7)).trim().slice(0, 7);
      const key = 'SHIFT_SCHEDULE_' + month;
      const em = await db('GET', 'employees', { select: 'employee_id,nama,email,divisi,jabatan,is_active', order: 'nama.asc', limit: 5000 });
      if (!em.ok) return json(res, 500, { ok: false, message: 'Gagal ambil employee list.', error: em.error });
      const r = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil jadwal bulanan.', error: r.error });
      const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
      const value = row ? safeJsonParse(row.value, {}) : {};
      const mirror = await db('GET', 'config', { select: 'key', key: 'like.' + ('SHIFT_SCHEDULE_EMP_' + month + '_') + '%', limit: 5000 });
      const mirrorCount = mirror.ok && Array.isArray(mirror.data) ? mirror.data.length : 0;
      const defs = await readScheduleDefaultRules();
      return json(res, 200, {
        ok: true,
        month: month,
        schedule_source: 'supabase.config',
        schedule_storage: { monthly_key: 'SHIFT_SCHEDULE_' + month, employee_row_prefix: 'SHIFT_SCHEDULE_EMP_' + month + '_', employee_rows: mirrorCount },
        shifts: ['PAGI', 'SORE', 'MALAM', 'FLX'],
        employees: em.data || [],
        templates: value.templates || {},
        defaults: defs.ok ? defs.rules : {},
        defaults_updated_at: defs.ok ? defs.updated_at : '',
        defaults_updated_by: defs.ok ? defs.updated_by : '',
        published_at: value.published_at || '',
        notes: value.notes || ''
      });
    }

    if (path === 'admin/schedules/monthly' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const month = String(b.month || ymd().slice(0, 7)).trim().slice(0, 7);
      if (!/^\d{4}-\d{2}$/.test(month)) return json(res, 400, { ok: false, message: 'Format month harus YYYY-MM.' });
      const templates = (b.templates && typeof b.templates === 'object') ? b.templates : {};
      const payload = {
        key: 'SHIFT_SCHEDULE_' + month,
        value: JSON.stringify({
          month: month,
          templates: templates,
          notes: String(b.notes || '').trim(),
          updated_by: a.email,
          updated_at: nowIso(),
          published_at: String(b.published_at || '')
        })
      };
      const up = await db('POST', 'config', { on_conflict: 'key' }, payload, { Prefer: 'resolution=merge-duplicates,return=minimal' });
      if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal simpan jadwal bulanan.', error: up.error });
      const mirrorSaved = await saveScheduleEmployeeRows(month, templates, a.email);
      await auditLog(a.email, 'UPSERT', 'shift_schedule', 'Simpan jadwal bulanan ' + month, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Jadwal bulanan berhasil disimpan di Supabase.', month: month, schedule_source: 'supabase.config', schedule_storage: { monthly_key: payload.key, employee_row_prefix: 'SHIFT_SCHEDULE_EMP_' + month + '_', mirror: mirrorSaved } });
    }

    if (path === 'admin/schedules/monthly/publish' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const month = String(b.month || ymd().slice(0, 7)).trim().slice(0, 7);
      if (!/^\d{4}-\d{2}$/.test(month)) return json(res, 400, { ok: false, message: 'Format month harus YYYY-MM.' });
      const key = 'SHIFT_SCHEDULE_' + month;
      const r = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil jadwal untuk publish.', error: r.error });
      const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
      if (!row) return json(res, 400, { ok: false, message: 'Jadwal bulan ini belum disimpan.' });
      const value = safeJsonParse(row.value, {}) || {};
      value.published_at = nowIso();
      value.published_by = a.email;
      const up = await db('POST', 'config', { on_conflict: 'key' }, { key: key, value: JSON.stringify(value) }, { Prefer: 'resolution=merge-duplicates,return=minimal' });
      if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal update status publish.', error: up.error });
      const ann = {
        announcement_id: rid('ANN'),
        judul: 'Jadwal Kerja Bulanan ' + month + ' Dipublikasikan',
        isi: 'Jadwal kerja bulan ' + month + ' sudah dipublikasikan. Silakan cek menu Jadwal Kerja Anda.',
        target_role: 'employee',
        published_at: nowIso(),
        expired_at: null,
        is_active: true,
        created_by: a.email
      };
      const ins = await db('POST', 'announcements', null, ann, { Prefer: 'return=minimal' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal kirim notifikasi jadwal.', error: ins.error });
      await auditLog(a.email, 'PUBLISH', 'shift_schedule', 'Publish jadwal bulanan ' + month, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Jadwal bulanan dipublikasikan dan notifikasi terkirim.', month: month, published_at: value.published_at });
    }

    if (path === 'admin/schedules/defaults' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const defs = await readScheduleDefaultRules();
      if (!defs.ok) return json(res, 500, { ok: false, message: defs.message, error: defs.error });
      return json(res, 200, { ok: true, rules: defs.rules, updated_at: defs.updated_at, updated_by: defs.updated_by, source: 'supabase.config' });
    }

    if (path === 'admin/schedules/defaults' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const rules = normalizeScheduleRulesMap(b.rules || {});
      const saved = await saveScheduleDefaultRules(rules, a.email);
      if (!saved.ok) return json(res, 500, { ok: false, message: saved.message, error: saved.error });
      await auditLog(a.email, 'UPSERT', 'shift_schedule_defaults', 'Simpan default schedule per divisi', String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Default schedule berhasil disimpan ke Supabase.', total_rules: Object.keys(rules).length });
    }

    if (path === 'admin/schedules/monthly/apply-defaults' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const month = String(b.month || ymd().slice(0, 7)).trim().slice(0, 7);
      if (!/^\d{4}-\d{2}$/.test(month)) return json(res, 400, { ok: false, message: 'Format month harus YYYY-MM.' });
      const overwrite = String(b.overwrite || 'true').toLowerCase() !== 'false';
      const defs = await readScheduleDefaultRules();
      if (!defs.ok) return json(res, 500, { ok: false, message: defs.message, error: defs.error });
      const rules = defs.rules || {};
      const key = 'SHIFT_SCHEDULE_' + month;
      const curr = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
      if (!curr.ok) return json(res, 500, { ok: false, message: 'Gagal membaca draft schedule.', error: curr.error });
      const row = Array.isArray(curr.data) && curr.data[0] ? curr.data[0] : null;
      const val = row ? safeJsonParse(row.value, {}) : {};
      const templates = (val.templates && typeof val.templates === 'object') ? val.templates : {};
      const em = await db('GET', 'employees', { select: 'employee_id,divisi,is_active', limit: 5000 });
      if (!em.ok) return json(res, 500, { ok: false, message: 'Gagal membaca employee.', error: em.error });
      const impact = computeScheduleDefaultsImpact(rules, templates, em.data || [], overwrite);
      val.month = month;
      val.templates = impact.templates;
      val.updated_by = a.email;
      val.updated_at = nowIso();
      const up = await db('POST', 'config', { on_conflict: 'key' }, { key: key, value: JSON.stringify(val) }, { Prefer: 'resolution=merge-duplicates,return=minimal' });
      if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal menyimpan hasil apply defaults.', error: up.error });
      const mirrorSaved = await saveScheduleEmployeeRows(month, impact.templates, a.email);
      await auditLog(a.email, 'UPSERT', 'shift_schedule', 'Apply defaults ke jadwal bulan ' + month + ', overwrite=' + String(overwrite) + ', applied=' + String(impact.applied_count), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Defaults berhasil diterapkan ke draft jadwal.', month: month, applied_count: impact.applied_count, skipped_existing: impact.skipped_existing, skipped_no_rule: impact.skipped_no_rule, skipped_inactive: impact.skipped_inactive, by_division: impact.by_division, overwrite: overwrite, schedule_source: 'supabase.config', schedule_storage: { monthly_key: key, employee_row_prefix: 'SHIFT_SCHEDULE_EMP_' + month + '_', mirror: mirrorSaved } });
    }

    if (path === 'admin/schedules/monthly/preview-defaults' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const month = String((req.query && req.query.month) || ymd().slice(0, 7)).trim().slice(0, 7);
      if (!/^\d{4}-\d{2}$/.test(month)) return json(res, 400, { ok: false, message: 'Format month harus YYYY-MM.' });
      const overwrite = String((req.query && req.query.overwrite) || 'true').toLowerCase() !== 'false';
      const defs = await readScheduleDefaultRules();
      if (!defs.ok) return json(res, 500, { ok: false, message: defs.message, error: defs.error });
      const key = 'SHIFT_SCHEDULE_' + month;
      const curr = await db('GET', 'config', { select: 'value', key: 'eq.' + key, limit: 1 });
      if (!curr.ok) return json(res, 500, { ok: false, message: 'Gagal membaca draft schedule.', error: curr.error });
      const row = Array.isArray(curr.data) && curr.data[0] ? curr.data[0] : null;
      const val = row ? safeJsonParse(row.value, {}) : {};
      const templates = (val.templates && typeof val.templates === 'object') ? val.templates : {};
      const em = await db('GET', 'employees', { select: 'employee_id,divisi,is_active', limit: 5000 });
      if (!em.ok) return json(res, 500, { ok: false, message: 'Gagal membaca employee.', error: em.error });
      const impact = computeScheduleDefaultsImpact(defs.rules || {}, templates, em.data || [], overwrite);
      return json(res, 200, { ok: true, month: month, overwrite: overwrite, default_rules: Object.keys(defs.rules || {}).length, applied_count: impact.applied_count, skipped_existing: impact.skipped_existing, skipped_no_rule: impact.skipped_no_rule, skipped_inactive: impact.skipped_inactive, by_division: impact.by_division, schedule_source: 'supabase.config' });
    }

    if (path === 'admin/schedules/monthly/rows' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const month = String((req.query && req.query.month) || ymd().slice(0, 7)).trim().slice(0, 7);
      if (!/^\d{4}-\d{2}$/.test(month)) return json(res, 400, { ok: false, message: 'Format month harus YYYY-MM.' });
      const prefix = 'SHIFT_SCHEDULE_EMP_' + month + '_';
      const rows = await db('GET', 'config', { select: 'key,value', key: 'like.' + prefix + '%', limit: 5000 });
      if (!rows.ok) return json(res, 500, { ok: false, message: 'Gagal membaca schedule rows.', error: rows.error });
      const out = (Array.isArray(rows.data) ? rows.data : []).map(function(r) {
        const v = safeJsonParse(r.value, {});
        return {
          key: String(r.key || ''),
          month: String(v.month || month),
          employee_id: String(v.employee_id || ''),
          shift_code: normalizeShiftCode(String(v.shift_code || 'PAGI')),
          off_saturday: !!v.off_saturday,
          off_sunday: !!v.off_sunday,
          updated_at: String(v.updated_at || ''),
          updated_by: String(v.updated_by || '')
        };
      });
      return json(res, 200, { ok: true, month: month, source: 'supabase.config', row_prefix: prefix, total: out.length, rows: out });
    }

    if (path === 'admin/master/sync-from-employees' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await syncDivisionPositionFromEmployees(a.email);
      if (!r.ok) return json(res, 500, { ok: false, message: r.message || 'Gagal sinkronisasi master.', error: r.error });
      return json(res, 200, { ok: true, message: 'Sinkronisasi master divisi/jabatan selesai.', result: r });
    }

    if (path === 'admin/attendance/today' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await db('GET', 'attendance', { select: '*', tanggal: 'eq.' + String(req.query.tanggal || ymd()), order: 'created_at.desc', limit: Math.min(Number(req.query.limit || 500), 2000) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil attendance today.', error: r.error });
      const rows = [];
      for (const x of (r.data || [])) {
        const meta = await attendanceMetaGet(x.attendance_id);
        const ws = effectiveWorkSeconds(x.jam_masuk, x.jam_keluar, meta, hms());
        const wm = Math.floor(ws / 60);
        const bs = Number(meta.break_total_seconds || (Number(meta.break_total_minutes || 0) * 60) || 0);
        rows.push(Object.assign({}, x, {
          work_seconds: ws,
          work_minutes: wm,
          work_duration_digital: workDurationDigital(ws),
          work_duration: workDurationLabel(wm),
          break_total_seconds: bs,
          break_total_minutes: Number(meta.break_total_minutes || 0),
          break_duration_digital: workDurationDigital(bs),
          break_active: !!meta.break_active_start,
          break_active_start: String(meta.break_active_start || ''),
          shift_code: String(meta.shift_code || '')
        }));
      }
      return json(res, 200, rows);
    }

    if (path === 'admin/reports/status/export-csv' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const tanggal = String((await readBody(req)).tanggal || ymd()).trim();
      const [rEmp, rAtt] = await Promise.all([
        db('GET', 'employees', { select: 'employee_id,nama', order: 'employee_id.asc', limit: 5000 }),
        db('GET', 'attendance', { select: '*', tanggal: 'eq.' + tanggal, order: 'created_at.desc', limit: 5000 })
      ]);
      if (!rEmp.ok || !rAtt.ok) return json(res, 500, { ok: false, message: 'Gagal generate export status CSV.', error: (!rEmp.ok ? rEmp.error : rAtt.error) });
      const attMap = {};
      (rAtt.data || []).forEach(function(x) { if (!attMap[x.employee_id]) attMap[x.employee_id] = x; });
      const lines = ['tanggal,employee_id,nama,status,shift,jam_masuk,jam_keluar,jam_kerja_digital,istirahat_digital'];
      for (const e of (rEmp.data || [])) {
        const arow = attMap[e.employee_id] || null;
        let work = '00:00:00';
        let brk = '00:00:00';
        let shift = '-';
        let statusLive = 'Belum Check In';
        if (arow) {
          const meta = await attendanceMetaGet(arow.attendance_id);
          const ws = effectiveWorkSeconds(arow.jam_masuk, arow.jam_keluar, meta, hms());
          const bs = Number(meta.break_total_seconds || (Number(meta.break_total_minutes || 0) * 60) || 0);
          work = workDurationDigital(ws);
          brk = workDurationDigital(bs);
          shift = String(meta.shift_code || '-');
          statusLive = !arow.jam_masuk ? 'Belum Check In' : (arow.jam_keluar ? 'Selesai Check Out' : (meta.break_active_start ? 'Sedang Istirahat' : 'Sedang Kerja'));
        }
        lines.push([
          tanggal,
          '"' + String(e.employee_id || '').replace(/"/g, '""') + '"',
          '"' + String(e.nama || '').replace(/"/g, '""') + '"',
          '"' + statusLive + '"',
          '"' + shift + '"',
          '"' + String((arow && arow.jam_masuk) || '-') + '"',
          '"' + String((arow && arow.jam_keluar) || '-') + '"',
          '"' + work + '"',
          '"' + brk + '"'
        ].join(','));
      }
      const fileName = 'status_karyawan_' + String(tanggal).replace(/-/g, '') + '.csv';
      const url = await uploadBufferToDrive(fileName, 'text/csv', Buffer.from(lines.join('\n'), 'utf8'), reportDriveFolderId());
      if (!url) return json(res, 500, { ok: false, message: 'Gagal upload CSV ke Drive.' });
      return json(res, 200, { ok: true, message: 'CSV status berhasil diekspor.', file_name: fileName, file_url: url });
    }

    if (path === 'admin/reports/status/export-pdf' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const tanggal = String((await readBody(req)).tanggal || ymd()).trim();
      const [rEmp, rAtt] = await Promise.all([
        db('GET', 'employees', { select: 'employee_id,nama', order: 'employee_id.asc', limit: 5000 }),
        db('GET', 'attendance', { select: '*', tanggal: 'eq.' + tanggal, order: 'created_at.desc', limit: 5000 })
      ]);
      if (!rEmp.ok || !rAtt.ok) return json(res, 500, { ok: false, message: 'Gagal generate export status PDF.', error: (!rEmp.ok ? rEmp.error : rAtt.error) });
      const attMap = {};
      (rAtt.data || []).forEach(function(x) { if (!attMap[x.employee_id]) attMap[x.employee_id] = x; });
      const rows = ['Tanggal: ' + tanggal, 'Format ringkas status karyawan', ''];
      for (const e of (rEmp.data || [])) {
        const arow = attMap[e.employee_id] || null;
        let work = '00:00:00';
        let brk = '00:00:00';
        let shift = '-';
        let statusLive = 'Belum Check In';
        if (arow) {
          const meta = await attendanceMetaGet(arow.attendance_id);
          const ws = effectiveWorkSeconds(arow.jam_masuk, arow.jam_keluar, meta, hms());
          const bs = Number(meta.break_total_seconds || (Number(meta.break_total_minutes || 0) * 60) || 0);
          work = workDurationDigital(ws);
          brk = workDurationDigital(bs);
          shift = String(meta.shift_code || '-');
          statusLive = !arow.jam_masuk ? 'Belum Check In' : (arow.jam_keluar ? 'Selesai Check Out' : (meta.break_active_start ? 'Sedang Istirahat' : 'Sedang Kerja'));
        }
        rows.push(String(e.employee_id || '-') + ' | ' + String(e.nama || '-') + ' | ' + statusLive + ' | Shift:' + shift + ' | Kerja:' + work + ' | Istirahat:' + brk);
      }
      const pdf = simplePdfBuffer('Laporan Status Karyawan ESS', rows);
      const fileName = 'status_karyawan_' + String(tanggal).replace(/-/g, '') + '.pdf';
      const url = await uploadBufferToDrive(fileName, 'application/pdf', pdf, reportDriveFolderId());
      if (!url) return json(res, 500, { ok: false, message: 'Gagal upload PDF ke Drive.' });
      return json(res, 200, { ok: true, message: 'PDF status berhasil diekspor.', file_name: fileName, file_url: url });
    }

    if (path === 'admin/leaves/pending' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const [r, rCfg] = await Promise.all([
        db('GET', 'leave_requests', { select: '*', status: 'eq.pending', order: 'created_at.asc', limit: Math.min(Number(req.query.limit || 300), 1000) }),
        db('GET', 'config', { select: 'key,value', key: 'in.(OPS_LEAVE_SLA_WARN_HOURS,OPS_LEAVE_SLA_CRITICAL_HOURS)', limit: 10 })
      ]);
      if (!r.ok || !rCfg.ok) return json(res, 500, { ok: false, message: 'Gagal ambil cuti pending.', error: (!r.ok ? r.error : rCfg.error) });
      const rows = r.data || [];
      const cfgMap = {};
      (rCfg.data || []).forEach(function(x) { cfgMap[String(x.key || '')] = Number(x.value || 0); });
      const warnH = Number(cfgMap.OPS_LEAVE_SLA_WARN_HOURS || 24);
      const criticalH = Number(cfgMap.OPS_LEAVE_SLA_CRITICAL_HOURS || 72);
      const ids = Array.from(new Set(rows.map(function(x) { return String(x.employee_id || '').trim(); }).filter(Boolean)));
      const empMap = {};
      if (ids.length > 0) {
        const em = await db('GET', 'employees', { select: 'employee_id,nama,email', employee_id: 'in.(' + ids.join(',') + ')' });
        if (em.ok) {
          (em.data || []).forEach(function(e) { empMap[String(e.employee_id || '')] = e; });
        }
      }
      const enriched = rows.map(function(x) {
        const e = empMap[String(x.employee_id || '')] || {};
        const ageHours = x.created_at ? Math.max(0, Math.floor((Date.now() - new Date(String(x.created_at)).getTime()) / 3600000)) : 0;
        let slaPriority = 'normal';
        if (ageHours >= criticalH) slaPriority = 'critical';
        else if (ageHours >= warnH) slaPriority = 'high';
        return Object.assign({}, x, {
          nama: String(e.nama || ''),
          email: String(x.email || e.email || ''),
          sla_age_hours: ageHours,
          sla_priority: slaPriority,
          sla_warn_hours: warnH,
          sla_critical_hours: criticalH
        });
      });
      enriched.sort(function(a1, b1) {
        const rank = function(v) { return v === 'critical' ? 3 : (v === 'high' ? 2 : 1); };
        if (rank(String(a1.sla_priority || 'normal')) !== rank(String(b1.sla_priority || 'normal'))) return rank(String(b1.sla_priority || 'normal')) - rank(String(a1.sla_priority || 'normal'));
        return Number(b1.sla_age_hours || 0) - Number(a1.sla_age_hours || 0);
      });
      return json(res, 200, enriched);
    }

    if (path === 'admin/leaves/escalation-matrix' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const pend = await db('GET', 'leave_requests', { select: 'leave_id,employee_id,created_at,status', status: 'eq.pending', order: 'created_at.asc', limit: 2000 });
      const cfg = await db('GET', 'config', { select: 'key,value', key: 'in.(OPS_LEAVE_SLA_WARN_HOURS,OPS_LEAVE_SLA_CRITICAL_HOURS)', limit: 10 });
      if (!pend.ok || !cfg.ok) return json(res, 500, { ok: false, message: 'Gagal ambil escalation matrix.', error: (!pend.ok ? pend.error : cfg.error) });
      const cfgMap = {};
      (cfg.data || []).forEach(function(x) { cfgMap[String(x.key || '')] = Number(x.value || 0); });
      const warnH = Number(cfgMap.OPS_LEAVE_SLA_WARN_HOURS || 24);
      const criticalH = Number(cfgMap.OPS_LEAVE_SLA_CRITICAL_HOURS || 72);
      const rows = (pend.data || []).map(function(x) {
        const age = x.created_at ? Math.max(0, Math.floor((Date.now() - new Date(String(x.created_at)).getTime()) / 3600000)) : 0;
        return { leave_id: x.leave_id, employee_id: x.employee_id, age_hours: age, priority: age >= criticalH ? 'critical' : (age >= warnH ? 'high' : 'normal') };
      });
      const summary = {
        pending_total: rows.length,
        pending_normal: rows.filter(function(x) { return x.priority === 'normal'; }).length,
        pending_high: rows.filter(function(x) { return x.priority === 'high'; }).length,
        pending_critical: rows.filter(function(x) { return x.priority === 'critical'; }).length,
        sla_warn_hours: warnH,
        sla_critical_hours: criticalH
      };
      return json(res, 200, { ok: true, summary: summary, top_overdue: rows.sort(function(a1, b1) { return Number(b1.age_hours || 0) - Number(a1.age_hours || 0); }).slice(0, 20) });
    }

    if (path === 'admin/leaves/sla-escalate' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const publishDigest = String(b.publish_digest || '').toLowerCase() === 'true' || b.publish_digest === true;
      const [pend, cfg] = await Promise.all([
        db('GET', 'leave_requests', { select: 'leave_id,employee_id,created_at,status', status: 'eq.pending', order: 'created_at.asc', limit: 3000 }),
        db('GET', 'config', { select: 'key,value', key: 'in.(OPS_LEAVE_SLA_WARN_HOURS,OPS_LEAVE_SLA_CRITICAL_HOURS)', limit: 10 })
      ]);
      if (!pend.ok || !cfg.ok) return json(res, 500, { ok: false, message: 'Gagal menjalankan SLA escalation.', error: (!pend.ok ? pend.error : cfg.error) });
      const cfgMap = {};
      (cfg.data || []).forEach(function(x) { cfgMap[String(x.key || '')] = Number(x.value || 0); });
      const warnH = Number(cfgMap.OPS_LEAVE_SLA_WARN_HOURS || 24);
      const criticalH = Number(cfgMap.OPS_LEAVE_SLA_CRITICAL_HOURS || 72);
      const rows = (pend.data || []).map(function(x) {
        const age = x.created_at ? Math.max(0, Math.floor((Date.now() - new Date(String(x.created_at)).getTime()) / 3600000)) : 0;
        return { leave_id: x.leave_id, employee_id: x.employee_id, age_hours: age, priority: age >= criticalH ? 'critical' : (age >= warnH ? 'high' : 'normal') };
      });
      const summary = {
        pending_total: rows.length,
        pending_normal: rows.filter(function(x) { return x.priority === 'normal'; }).length,
        pending_high: rows.filter(function(x) { return x.priority === 'high'; }).length,
        pending_critical: rows.filter(function(x) { return x.priority === 'critical'; }).length,
        sla_warn_hours: warnH,
        sla_critical_hours: criticalH
      };
      const actions = [];
      if (summary.pending_critical > 0) actions.push({ priority: 1, type: 'critical', title: 'Tangani seluruh pending kritikal', detail: summary.pending_critical + ' pengajuan melewati SLA kritikal. Jalankan batch approval/reject segera.' });
      if (summary.pending_high > 0) actions.push({ priority: 2, type: 'high', title: 'Kurangi backlog high SLA', detail: summary.pending_high + ' pengajuan melewati SLA warning. Prioritaskan dalam 1 siklus review berikutnya.' });
      if (summary.pending_total > 0) actions.push({ priority: 3, type: 'normal', title: 'Sinkronkan jadwal reviewer', detail: 'Atur slot review berkala agar antrian tidak menumpuk kembali.' });
      if (!actions.length) actions.push({ priority: 1, type: 'stable', title: 'SLA Stabil', detail: 'Tidak ada pending leave yang melewati threshold SLA.' });
      let announcement = null;
      if (publishDigest) {
        const title = summary.pending_critical > 0 ? 'SLA Leave Escalation: Critical Queue' : (summary.pending_high > 0 ? 'SLA Leave Escalation: Warning Queue' : 'SLA Leave Escalation: Stable');
        const detail = 'Pending=' + summary.pending_total + ' | Critical=' + summary.pending_critical + ' | High=' + summary.pending_high + ' | Threshold=' + warnH + 'h/' + criticalH + 'h';
        const payload = {
          announcement_id: rid('ANN'),
          judul: '[OPS SLA] ' + title,
          isi: detail + '\n\nSumber: Leave SLA Escalation Workflow',
          target_role: 'admin',
          published_at: nowIso(),
          expired_at: null,
          is_active: true,
          created_by: a.email
        };
        const ins = await db('POST', 'announcements', null, payload, { Prefer: 'return=representation' });
        if (ins.ok) announcement = (ins.data && ins.data[0]) || null;
      }
      await auditLog(a.email, 'RUN', 'leave_sla_escalation', 'Run leave SLA escalation workflow', String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, summary: summary, actions: actions, top_overdue: rows.sort(function(a1, b1) { return Number(b1.age_hours || 0) - Number(a1.age_hours || 0); }).slice(0, 30), digest_published: !!announcement, announcement: announcement });
    }

    if (path === 'admin/control-tower/summary' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const today = ymd();
      const currentYear = String(today).slice(0, 4);
      let emp = await db('GET', 'employees', { select: 'employee_id,nama,email,divisi,jabatan,is_active,tanggal_lahir,jatah_cuti,sisa_cuti', order: 'nama.asc', limit: 5000 });
      if (!emp.ok) {
        // Fallback for deployments where leave balance columns are not yet present.
        emp = await db('GET', 'employees', { select: 'employee_id,nama,email,divisi,jabatan,is_active,tanggal_lahir', order: 'nama.asc', limit: 5000 });
      }
      if (!emp.ok) return json(res, 500, { ok: false, message: 'Gagal ambil control tower summary.', error: emp.error });
      const activeRows = (emp.data || []).filter(function(e) { return String(e.is_active).toLowerCase() === 'true'; });
      const approvedLeavesReq = await db('GET', 'leave_requests', {
        select: 'leave_id,employee_id,jenis_cuti,tanggal_mulai,tanggal_selesai,status,approved_at',
        status: 'eq.approved',
        tanggal_mulai: 'gte.' + currentYear + '-01-01',
        order: 'tanggal_mulai.asc',
        limit: 8000
      });
      if (!approvedLeavesReq.ok) return json(res, 500, { ok: false, message: 'Gagal ambil data leave approved.', error: approvedLeavesReq.error });
      const md = today.slice(5); // MM-DD
      const monthPrefix = today.slice(0, 7); // YYYY-MM
      const birthdaysToday = activeRows.filter(function(e) {
        const d = String(e.tanggal_lahir || '');
        if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return false;
        return d.slice(5) === md;
      }).map(function(e) {
        const birthYear = Number(String(e.tanggal_lahir || '').slice(0, 4) || 0);
        const age = birthYear > 0 ? (Number(today.slice(0, 4)) - birthYear) : 0;
        return {
          employee_id: String(e.employee_id || ''),
          nama: String(e.nama || ''),
          email: String(e.email || ''),
          divisi: String(e.divisi || ''),
          jabatan: String(e.jabatan || ''),
          tanggal_lahir: String(e.tanggal_lahir || ''),
          usia: age > 0 ? age : null
        };
      });
      const monthNames = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
      const monthMap = {};
      activeRows.forEach(function(e) {
        const d = String(e.tanggal_lahir || '');
        if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return;
        const m = Number(d.slice(5, 7) || 0);
        const day = Number(d.slice(8, 10) || 0);
        if (m < 1 || m > 12 || day < 1 || day > 31) return;
        const k = String(m);
        if (!monthMap[k]) monthMap[k] = { month: m, month_name: monthNames[m - 1], total: 0, items: [] };
        monthMap[k].total += 1;
        monthMap[k].items.push({
          employee_id: String(e.employee_id || ''),
          nama: String(e.nama || ''),
          divisi: String(e.divisi || ''),
          jabatan: String(e.jabatan || ''),
          tanggal_lahir: d,
          day: day
        });
      });
      const birthdaysCalendar = Object.values(monthMap).map(function(x) {
        x.items.sort(function(a1, b1) { return Number(a1.day || 0) - Number(b1.day || 0); });
        return x;
      }).sort(function(a1, b1) { return Number(a1.month || 0) - Number(b1.month || 0); });
      const empNameMap = {};
      activeRows.forEach(function(e) { empNameMap[String(e.employee_id || '')] = String(e.nama || ''); });
      const leaveMonthMap = {};
      (approvedLeavesReq.data || []).forEach(function(r) {
        const ds = String(r.tanggal_mulai || '');
        if (!/^\d{4}-\d{2}-\d{2}$/.test(ds)) return;
        const m = Number(ds.slice(5, 7) || 0);
        const day = Number(ds.slice(8, 10) || 0);
        if (m < 1 || m > 12 || day < 1 || day > 31) return;
        const k = String(m);
        if (!leaveMonthMap[k]) leaveMonthMap[k] = { month: m, month_name: monthNames[m - 1], total: 0, items: [] };
        leaveMonthMap[k].total += 1;
        leaveMonthMap[k].items.push({
          leave_id: String(r.leave_id || ''),
          employee_id: String(r.employee_id || ''),
          nama: String(empNameMap[String(r.employee_id || '')] || r.employee_id || '-'),
          jenis_cuti: String(r.jenis_cuti || ''),
          tanggal_mulai: String(r.tanggal_mulai || ''),
          tanggal_selesai: String(r.tanggal_selesai || ''),
          day: day
        });
      });
      const approvedLeavesCalendar = Object.values(leaveMonthMap).map(function(x) {
        x.items.sort(function(a1, b1) { return Number(a1.day || 0) - Number(b1.day || 0); });
        return x;
      }).sort(function(a1, b1) { return Number(a1.month || 0) - Number(b1.month || 0); });
      const birthdaysThisMonthCount = activeRows.filter(function(e) {
        const d = String(e.tanggal_lahir || '');
        if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return false;
        return d.slice(5, 7) === monthPrefix.slice(5, 7);
      }).length;
      const missingBirthdateCount = activeRows.filter(function(e) {
        const d = String(e.tanggal_lahir || '');
        return !/^\d{4}-\d{2}-\d{2}$/.test(d);
      }).length;
      const leaveBalanceRows = activeRows.map(function(e) {
        return {
          employee_id: String(e.employee_id || ''),
          nama: String(e.nama || ''),
          divisi: String(e.divisi || ''),
          jabatan: String(e.jabatan || ''),
          jatah_cuti: Number(e.jatah_cuti || 0),
          sisa_cuti: Number(e.sisa_cuti || 0)
        };
      }).sort(function(a1, b1) { return Number(a1.sisa_cuti || 0) - Number(b1.sisa_cuti || 0); }).slice(0, 300);
      const fileLocations = [
        { key: 'attendance', label: 'File Absensi (Foto Check-in/Check-out)', folder_id: reportDriveFolderId() },
        { key: 'payslip', label: 'File Payslip', folder_id: payrollDriveFolderId() },
        { key: 'sick_letter', label: 'File Surat Sakit (Lampiran Cuti)', folder_id: leaveDriveFolderId() }
      ].map(function(x) {
        const fid = String(x.folder_id || '');
        return Object.assign({}, x, { drive_url: fid ? ('https://drive.google.com/drive/folders/' + fid) : '' });
      });
      return json(res, 200, {
        ok: true,
        date: today,
        metrics: {
          active_employees: activeRows.length,
          birthdays_today: birthdaysToday.length,
          birthdays_this_month: birthdaysThisMonthCount,
          missing_birthdate: missingBirthdateCount,
          approved_leaves_year: approvedLeavesReq.data ? approvedLeavesReq.data.length : 0
        },
        birthdays_today: birthdaysToday,
        birthdays_calendar: birthdaysCalendar,
        approved_leaves_calendar: approvedLeavesCalendar,
        file_locations: fileLocations,
        leave_balance_rows: leaveBalanceRows
      });
    }

    if (path === 'admin/control-tower/execute' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const publishOpsDigest = String(b.publish_ops_digest || '').toLowerCase() === 'true' || b.publish_ops_digest === true;
      const publishLeaveDigest = String(b.publish_leave_digest || '').toLowerCase() === 'true' || b.publish_leave_digest === true;
      const sendCheckinAlert = String(b.send_checkin_alert || '').toLowerCase() === 'true' || b.send_checkin_alert === true;
      const today = ymd();
      const summaryReq = await db('GET', 'leave_requests', { select: 'leave_id,created_at,status', status: 'eq.pending', limit: 3000 });
      if (!summaryReq.ok) return json(res, 500, { ok: false, message: 'Gagal eksekusi control tower.', error: summaryReq.error });
      const pendingCount = (summaryReq.data || []).length;
      let opsAnnouncement = null;
      let leaveAnnouncement = null;
      let checkinAlertResult = { target_count: 0, sent_count: 0, failed_count: 0 };
      if (publishOpsDigest) {
        const p = {
          announcement_id: rid('ANN'),
          judul: '[OPS CT] Daily Ops Digest ' + today,
          isi: 'Control Tower menjalankan orkestrasi harian. Pending leave saat ini: ' + pendingCount + '.',
          target_role: 'admin',
          published_at: nowIso(),
          expired_at: null,
          is_active: true,
          created_by: a.email
        };
        const ins = await db('POST', 'announcements', null, p, { Prefer: 'return=representation' });
        if (ins.ok) opsAnnouncement = (ins.data && ins.data[0]) || null;
      }
      if (publishLeaveDigest) {
        const p2 = {
          announcement_id: rid('ANN'),
          judul: '[OPS CT] Leave SLA Digest ' + today,
          isi: 'Control Tower mengeksekusi SLA leave digest. Total pending: ' + pendingCount + '.',
          target_role: 'admin',
          published_at: nowIso(),
          expired_at: null,
          is_active: true,
          created_by: a.email
        };
        const ins2 = await db('POST', 'announcements', null, p2, { Prefer: 'return=representation' });
        if (ins2.ok) leaveAnnouncement = (ins2.data && ins2.data[0]) || null;
      }
      if (sendCheckinAlert) {
        const [emp, att, leaveApproved] = await Promise.all([
          db('GET', 'employees', { select: 'employee_id,nama,email,is_active', limit: 5000 }),
          db('GET', 'attendance', { select: 'employee_id', tanggal: 'eq.' + today, limit: 10000 }),
          db('GET', 'leave_requests', { select: 'employee_id', status: 'eq.approved', and: '(tanggal_mulai.lte.' + today + ',tanggal_selesai.gte.' + today + ')', limit: 5000 })
        ]);
        if (emp.ok && att.ok && leaveApproved.ok) {
          const checkedSet = new Set((att.data || []).map(function(x) { return String(x.employee_id || ''); }).filter(Boolean));
          const leaveSet = new Set((leaveApproved.data || []).map(function(x) { return String(x.employee_id || ''); }).filter(Boolean));
          const targets = (emp.data || []).filter(function(e) {
            const id = String(e.employee_id || '');
            if (String(e.is_active).toLowerCase() !== 'true') return false;
            if (!id || checkedSet.has(id) || leaveSet.has(id)) return false;
            return isValidEmail(String(e.email || '').trim().toLowerCase());
          });
          checkinAlertResult.target_count = targets.length;
          for (const e of targets) {
            const html = leaveMailTemplate('Reminder Check-in ESS', [
              'Halo ' + String(e.nama || e.employee_id) + ',',
              'Sampai saat ini sistem belum mencatat check-in Anda untuk tanggal <b>' + today + '</b>.',
              'Silakan login ESS dan lakukan check-in sesuai jadwal kerja.'
            ]);
            const rs = await sendEssEmail(String(e.email || ''), 'Reminder Check-in ESS - ' + today, html);
            if (rs && rs.sent) checkinAlertResult.sent_count += 1;
            else checkinAlertResult.failed_count += 1;
          }
        }
      }
      await auditLog(a.email, 'RUN', 'control_tower', 'Execute workforce control tower workflow', String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, {
        ok: true,
        message: 'Control Tower workflow selesai.',
        result: {
          pending_leaves: pendingCount,
          ops_digest_published: !!opsAnnouncement,
          leave_digest_published: !!leaveAnnouncement,
          checkin_alert_sent: checkinAlertResult.sent_count,
          checkin_alert_target: checkinAlertResult.target_count,
          checkin_alert_failed: checkinAlertResult.failed_count
        },
        announcements: [opsAnnouncement, leaveAnnouncement].filter(Boolean)
      });
    }

    if (path === 'admin/control-tower/birthday-broadcast' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const today = ymd();
      const emp = await db('GET', 'employees', { select: 'employee_id,nama,email,is_active,tanggal_lahir,divisi,jabatan', limit: 5000 });
      if (!emp.ok) return json(res, 500, { ok: false, message: 'Gagal ambil data karyawan.', error: emp.error });
      const activeRows = (emp.data || []).filter(function(e) { return String(e.is_active).toLowerCase() === 'true'; });
      const md = today.slice(5);
      const birthdaysToday = activeRows.filter(function(e) {
        const d = String(e.tanggal_lahir || '');
        return /^\d{4}-\d{2}-\d{2}$/.test(d) && d.slice(5) === md;
      });
      const recipientRows = activeRows.filter(function(e) { return isValidEmail(String(e.email || '').trim().toLowerCase()); });
      const customMessage = String(b.message || '').trim();
      const names = birthdaysToday.map(function(e) { return String(e.nama || e.employee_id || '-'); });
      const title = 'Ucapan Ulang Tahun Karyawan - ' + today;
      const lines = [
        'Halo rekan-rekan,',
        birthdaysToday.length
          ? ('Hari ini kita merayakan ulang tahun: <b>' + names.join(', ') + '</b>. Selamat ulang tahun, semoga sehat selalu dan sukses!')
          : 'Tidak ada data ulang tahun hari ini.',
        customMessage || 'Salam hangat dari tim HR.'
      ];
      const html = leaveMailTemplate('Broadcast Ulang Tahun Karyawan', lines);
      let sent = 0;
      let failed = 0;
      for (const e of recipientRows) {
        const rs = await sendEssEmail(String(e.email || ''), title, html);
        if (rs && rs.sent) sent += 1; else failed += 1;
      }
      await auditLog(a.email, 'CREATE', 'birthday_broadcast', 'Broadcast ulang tahun ke ' + String(recipientRows.length) + ' karyawan', String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, {
        ok: true,
        message: 'Broadcast ulang tahun selesai.',
        date: today,
        birthdays_today: birthdaysToday.length,
        birthday_names: names,
        recipients: recipientRows.length,
        sent: sent,
        failed: failed
      });
    }

    if (path === 'admin/leaves' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const q = { select: '*', order: 'created_at.desc', limit: Math.min(Number(req.query.limit || 500), 2000) };
      if (req.query.status) q.status = 'eq.' + String(req.query.status).toLowerCase();
      const r = await db('GET', 'leave_requests', q);
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil leaves.', error: r.error });
      const rows = r.data || [];
      const ids = Array.from(new Set(rows.map(function(x) { return String(x.employee_id || '').trim(); }).filter(Boolean)));
      const empMap = {};
      if (ids.length > 0) {
        const em = await db('GET', 'employees', { select: 'employee_id,nama,email', employee_id: 'in.(' + ids.join(',') + ')' });
        if (em.ok) {
          (em.data || []).forEach(function(e) { empMap[String(e.employee_id || '')] = e; });
        }
      }
      const enriched = rows.map(function(x) {
        const e = empMap[String(x.employee_id || '')] || {};
        return Object.assign({}, x, {
          nama: String(e.nama || ''),
          email: String(x.email || e.email || '')
        });
      });
      return json(res, 200, enriched);
    }

    if (path === 'admin/leaves/apply' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const me = { employee_id: a.employee_id, email: a.email, role: a.role };
      const leaveTypeName = String(b.jenis_cuti || '').trim();
      const lt = leaveTypeName ? await db('GET', 'leave_types', { select: 'leave_type_id,nama_jenis_cuti,requires_attachment', nama_jenis_cuti: 'eq.' + leaveTypeName, limit: 1 }) : { ok: true, data: [] };
      if (!lt.ok) return json(res, 500, { ok: false, message: 'Gagal validasi jenis cuti.', error: lt.error });
      const ltRow = Array.isArray(lt.data) && lt.data[0] ? lt.data[0] : null;
      if (!ltRow) return json(res, 400, { ok: false, message: 'Jenis cuti tidak valid atau tidak aktif.' });
      const requireAttachment = !!(ltRow && (ltRow.requires_attachment === true || String(ltRow.requires_attachment).toLowerCase() === 'true'));
      const sourceAttachmentUrl = String(b.lampiran_url || '').trim() || toDataUrlFromFileObject(b.attachment);
      const hasAttachmentObj = !!(b.attachment && String((b.attachment || {}).base64Data || '').trim());
      if (requireAttachment && !sourceAttachmentUrl && !hasAttachmentObj) return json(res, 400, { ok: false, message: 'Lampiran wajib untuk jenis cuti ini.' });
      const employeeName = await getEmployeeDisplayName(me);
      const driveAttachmentUrl = hasAttachmentObj ? await tryUploadLeaveAttachmentToDrive(b.attachment, { employee_id: me.employee_id, employee_name: employeeName, leave_type: leaveTypeName || 'cuti', tanggal: String(b.tanggal_mulai || ymd()), jam: hms() }) : '';
      const attachmentUrl = String(driveAttachmentUrl || sourceAttachmentUrl || '').trim();
      const days = Number(b.jumlah_hari || 0) > 0 ? Number(b.jumlah_hari || 0) : calcLeaveDays(b.tanggal_mulai, b.tanggal_selesai);
      const payload = { leave_id: rid('LEAVE'), employee_id: me.employee_id, email: me.email, jenis_cuti: leaveTypeName, tanggal_mulai: String(b.tanggal_mulai || '').trim(), tanggal_selesai: String(b.tanggal_selesai || '').trim(), jumlah_hari: days, alasan: String(b.alasan || '').trim(), lampiran_url: attachmentUrl, status: 'pending', approver_email: '', created_at: nowIso(), updated_at: nowIso() };
      if (!payload.jenis_cuti || !payload.tanggal_mulai || !payload.tanggal_selesai) return json(res, 400, { ok: false, message: 'jenis_cuti, tanggal_mulai, tanggal_selesai wajib diisi.' });
      if (!/^\d{4}-\d{2}-\d{2}$/.test(payload.tanggal_mulai) || !/^\d{4}-\d{2}-\d{2}$/.test(payload.tanggal_selesai)) return json(res, 400, { ok: false, message: 'Format tanggal harus YYYY-MM-DD.' });
      if (payload.tanggal_mulai > payload.tanggal_selesai) return json(res, 400, { ok: false, message: 'Tanggal mulai tidak boleh lebih besar dari tanggal selesai.' });
      if (Number(payload.jumlah_hari || 0) <= 0) return json(res, 400, { ok: false, message: 'Rentang tanggal cuti tidak valid.' });
      const ins = await db('POST', 'leave_requests', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal submit leave.', error: ins.error });
      try {
        const admins = await getAdminEmails();
        const title = 'Pengajuan Cuti Baru Perlu Persetujuan';
        const detailHtml = leaveMailTemplate(title, [
          'Halo Admin,',
          '<b>' + String(employeeName || me.employee_id) + '</b> mengajukan cuti.',
          'Jenis cuti: <b>' + payload.jenis_cuti + '</b>',
          'Tanggal: <b>' + payload.tanggal_mulai + '</b> s/d <b>' + payload.tanggal_selesai + '</b>'
        ]);
        for (const em of admins) await sendEssEmail(em, title + ' - ESS', detailHtml);
      } catch (_) {}
      return json(res, 200, { ok: true, message: 'Pengajuan cuti admin berhasil dikirim.', data: ins.data });
    }

    if (path === 'admin/leaves/approve' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const leaveId = String(b.leave_id || '').trim();
      if (!leaveId) return json(res, 400, { ok: false, message: 'leave_id wajib diisi.' });
      const cur = await db('GET', 'leave_requests', { select: 'leave_id,status', leave_id: 'eq.' + leaveId, limit: 1 });
      if (!cur.ok) return json(res, 500, { ok: false, message: 'Gagal validasi leave.', error: cur.error });
      const curRow = Array.isArray(cur.data) && cur.data[0] ? cur.data[0] : null;
      if (!curRow) return json(res, 404, { ok: false, message: 'Leave request tidak ditemukan.' });
      if (String(curRow.status || '').toLowerCase() !== 'pending') return json(res, 400, { ok: false, message: 'Leave request ini tidak dalam status pending.' });
      const p = await db('PATCH', 'leave_requests', { leave_id: 'eq.' + leaveId, status: 'eq.pending' }, { status: 'approved', approver_email: a.email, approved_at: nowIso(), catatan_approver: String(b.catatan_approver || '').trim(), updated_at: nowIso() }, { Prefer: 'return=representation' });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal approve.', error: p.error });
      if (!Array.isArray(p.data) || p.data.length === 0) return json(res, 409, { ok: false, message: 'Status leave sudah berubah. Silakan refresh data.' });
      try {
        const row = p.data[0] || {};
        const mail = leaveMailTemplate('Pengajuan Cuti Disetujui', [
          'Halo,',
          'Pengajuan cuti Anda telah <b>DISETUJUI</b>.',
          'Jenis cuti: <b>' + String(row.jenis_cuti || '-') + '</b>',
          'Tanggal: <b>' + String(row.tanggal_mulai || '-') + '</b> s/d <b>' + String(row.tanggal_selesai || '-') + '</b>',
          'Catatan approver: ' + String(row.catatan_approver || '-')
        ]);
        await sendEssEmail(String(row.email || ''), 'Pengajuan Cuti Disetujui - ESS', mail);
      } catch (_) {}
      await auditLog(a.email, 'APPROVE', 'leave_requests', 'Approve leave ' + leaveId, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Pengajuan cuti berhasil disetujui.', data: p.data });
    }

    if (path === 'admin/leaves/reject' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const leaveId = String(b.leave_id || '').trim();
      if (!leaveId) return json(res, 400, { ok: false, message: 'leave_id wajib diisi.' });
      const cur = await db('GET', 'leave_requests', { select: 'leave_id,status', leave_id: 'eq.' + leaveId, limit: 1 });
      if (!cur.ok) return json(res, 500, { ok: false, message: 'Gagal validasi leave.', error: cur.error });
      const curRow = Array.isArray(cur.data) && cur.data[0] ? cur.data[0] : null;
      if (!curRow) return json(res, 404, { ok: false, message: 'Leave request tidak ditemukan.' });
      if (String(curRow.status || '').toLowerCase() !== 'pending') return json(res, 400, { ok: false, message: 'Leave request ini tidak dalam status pending.' });
      const p = await db('PATCH', 'leave_requests', { leave_id: 'eq.' + leaveId, status: 'eq.pending' }, { status: 'rejected', approver_email: a.email, approved_at: nowIso(), catatan_approver: String(b.catatan_approver || '').trim(), updated_at: nowIso() }, { Prefer: 'return=representation' });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal reject.', error: p.error });
      if (!Array.isArray(p.data) || p.data.length === 0) return json(res, 409, { ok: false, message: 'Status leave sudah berubah. Silakan refresh data.' });
      try {
        const row = p.data[0] || {};
        const mail = leaveMailTemplate('Pengajuan Cuti Ditolak', [
          'Halo,',
          'Pengajuan cuti Anda telah <b>DITOLAK</b>.',
          'Jenis cuti: <b>' + String(row.jenis_cuti || '-') + '</b>',
          'Tanggal: <b>' + String(row.tanggal_mulai || '-') + '</b> s/d <b>' + String(row.tanggal_selesai || '-') + '</b>',
          'Catatan approver: ' + String(row.catatan_approver || '-')
        ]);
        await sendEssEmail(String(row.email || ''), 'Pengajuan Cuti Ditolak - ESS', mail);
      } catch (_) {}
      await auditLog(a.email, 'REJECT', 'leave_requests', 'Reject leave ' + leaveId, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Pengajuan cuti berhasil ditolak.', data: p.data });
    }

    if (path === 'admin/leaves/batch-action' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const action = String(b.action || '').trim().toLowerCase();
      const leaveIds = Array.isArray(b.leave_ids) ? b.leave_ids.map(function(x) { return String(x || '').trim(); }).filter(Boolean) : [];
      const note = String(b.catatan_approver || '').trim();
      if (!['approve', 'reject'].includes(action)) return json(res, 400, { ok: false, message: 'action wajib approve/reject.' });
      if (!leaveIds.length) return json(res, 400, { ok: false, message: 'leave_ids wajib diisi.' });
      const statusTarget = action === 'approve' ? 'approved' : 'rejected';
      const processed = [];
      const skipped = [];
      for (const leaveId of leaveIds) {
        const p = await db('PATCH', 'leave_requests', { leave_id: 'eq.' + leaveId, status: 'eq.pending' }, {
          status: statusTarget,
          approver_email: a.email,
          approved_at: nowIso(),
          catatan_approver: note,
          updated_at: nowIso()
        }, { Prefer: 'return=representation' });
        if (p.ok && Array.isArray(p.data) && p.data.length > 0) {
          processed.push(leaveId);
          try {
            const row = p.data[0] || {};
            const approved = action === 'approve';
            const mail = leaveMailTemplate(approved ? 'Pengajuan Cuti Disetujui' : 'Pengajuan Cuti Ditolak', [
              'Halo,',
              'Pengajuan cuti Anda telah <b>' + (approved ? 'DISETUJUI' : 'DITOLAK') + '</b>.',
              'Jenis cuti: <b>' + String(row.jenis_cuti || '-') + '</b>',
              'Tanggal: <b>' + String(row.tanggal_mulai || '-') + '</b> s/d <b>' + String(row.tanggal_selesai || '-') + '</b>',
              'Catatan approver: ' + String(row.catatan_approver || '-')
            ]);
            await sendEssEmail(String(row.email || ''), (approved ? 'Pengajuan Cuti Disetujui' : 'Pengajuan Cuti Ditolak') + ' - ESS', mail);
          } catch (_) {}
        } else skipped.push(leaveId);
      }
      await auditLog(a.email, action === 'approve' ? 'APPROVE_BATCH' : 'REJECT_BATCH', 'leave_requests', (action === 'approve' ? 'Approve' : 'Reject') + ' batch leaves ' + processed.join(','), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, {
        ok: true,
        message: 'Batch action selesai.',
        summary: { action: action, requested: leaveIds.length, processed: processed.length, skipped: skipped.length },
        processed_leave_ids: processed,
        skipped_leave_ids: skipped
      });
    }

    if (path === 'admin/announcements' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await db('GET', 'announcements', { select: '*', order: 'published_at.desc', limit: Math.min(Number(req.query.limit || 200), 1000) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil announcements.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'admin/announcements' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const payload = { announcement_id: rid('ANN'), judul: String(b.judul || '').trim(), isi: String(b.isi || '').trim(), target_role: String(b.target_role || 'all').trim().toLowerCase(), published_at: b.published_at || nowIso(), expired_at: b.expired_at || null, is_active: b.is_active === undefined ? true : String(b.is_active).toLowerCase() === 'true', created_by: a.email };
      if (!payload.judul || !payload.isi) return json(res, 400, { ok: false, message: 'judul dan isi wajib diisi.' });
      if (!['all', 'employee', 'admin'].includes(payload.target_role)) return json(res, 400, { ok: false, message: 'target_role tidak valid.' });
      const ins = await db('POST', 'announcements', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal membuat announcement.', error: ins.error });
      return json(res, 200, { ok: true, message: 'Announcement berhasil dibuat.', data: ins.data });
    }

    if (path === 'admin/announcements' && method === 'PATCH') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const announcementId = String(b.announcement_id || '').trim();
      if (!announcementId) return json(res, 400, { ok: false, message: 'announcement_id wajib diisi.' });
      const patch = { updated_at: nowIso() };
      if (b.judul !== undefined) patch.judul = String(b.judul || '').trim();
      if (b.isi !== undefined) patch.isi = String(b.isi || '').trim();
      if (b.target_role !== undefined) patch.target_role = String(b.target_role || '').trim().toLowerCase();
      if (b.published_at !== undefined) patch.published_at = b.published_at || nowIso();
      if (b.expired_at !== undefined) patch.expired_at = b.expired_at || null;
      if (b.is_active !== undefined) patch.is_active = String(b.is_active).toLowerCase() === 'true';
      if (patch.judul !== undefined && !patch.judul) return json(res, 400, { ok: false, message: 'judul tidak boleh kosong.' });
      if (patch.isi !== undefined && !patch.isi) return json(res, 400, { ok: false, message: 'isi tidak boleh kosong.' });
      if (patch.target_role !== undefined && !['all', 'employee', 'admin'].includes(patch.target_role)) return json(res, 400, { ok: false, message: 'target_role tidak valid.' });
      if (Object.keys(patch).length <= 1) return json(res, 400, { ok: false, message: 'Tidak ada field yang diupdate.' });
      const upd = await db('PATCH', 'announcements', { announcement_id: 'eq.' + announcementId }, patch, { Prefer: 'return=representation' });
      if (!upd.ok) return json(res, 500, { ok: false, message: 'Gagal update announcement.', error: upd.error });
      if (!Array.isArray(upd.data) || upd.data.length === 0) return json(res, 404, { ok: false, message: 'Announcement tidak ditemukan.' });
      await auditLog(a.email, 'UPDATE', 'announcements', 'Update announcement ' + announcementId, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Announcement berhasil diperbarui.', data: upd.data });
    }

    if (path === 'admin/payroll-docs' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const q = { select: '*', order: 'uploaded_at.desc', limit: Math.min(Number(req.query.limit || 300), 1000) };
      if (req.query.employee_id) q.employee_id = 'eq.' + String(req.query.employee_id).trim();
      if (req.query.bulan) q.bulan = 'eq.' + String(req.query.bulan).trim();
      if (req.query.tahun) q.tahun = 'eq.' + String(req.query.tahun).trim();
      const r = await db('GET', 'payroll_docs', q);
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil payroll docs.', error: r.error });
      let rows = r.data || [];
      const keyword = String(req.query.keyword || '').trim().toLowerCase();
      if (keyword) {
        rows = rows.filter(function(x) {
          const txt = (String(x.doc_id || '') + ' ' + String(x.employee_id || '') + ' ' + String(x.email || '') + ' ' + String(x.nama_file || '') + ' ' + String(x.keterangan || '')).toLowerCase();
          return txt.indexOf(keyword) >= 0;
        });
      }
      return json(res, 200, rows.map(enrichPayrollDoc));
    }

    if (path === 'admin/payroll-docs' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const employeeId = String(b.employee_id || '').trim();
      const givenEmail = String(b.email || '').trim().toLowerCase();
      let resolvedEmail = givenEmail;
      let employeeName = '';
      if (employeeId) {
        const em = await db('GET', 'employees', { select: 'nama,email', employee_id: 'eq.' + employeeId, limit: 1 });
        if (em.ok && Array.isArray(em.data) && em.data[0]) {
          resolvedEmail = resolvedEmail || String(em.data[0].email || '').trim().toLowerCase();
          employeeName = String(em.data[0].nama || '').trim();
        }
      }
      const nowDate = new Date();
      const prevMonthDate = new Date(nowDate.getFullYear(), nowDate.getMonth() - 1, 1);
      const defaultBulan = prevMonthDate.toLocaleString('id-ID', { month: 'long' });
      const defaultTahun = String(nowDate.getFullYear());
      const safeEmployeeName = String(employeeName || employeeId || 'Karyawan').replace(/[^\p{L}\p{N}\s_-]/gu, '').trim();
      const autoFileName = 'Slip Gaji ' + defaultBulan + ' ' + defaultTahun + ' - ' + safeEmployeeName;
      const payrollResult = payrollEngine({
        employee_id: employeeId,
        components: Array.isArray(b.components) ? b.components : undefined,
        gaji_pokok: b.gaji_pokok,
        basic_salary: b.basic_salary,
        full_salary: b.full_salary,
        tunjangan: b.tunjangan,
        allowance: b.allowance,
        transport_allowance: b.transport_allowance,
        meal_allowance: b.meal_allowance,
        lembur: b.lembur,
        overtime_pay: b.overtime_pay,
        bonus: b.bonus,
        attendance_allowance: b.attendance_allowance,
        thr: b.thr,
        potongan_pajak: b.potongan_pajak,
        tax: b.tax,
        bpjs_kesehatan: b.bpjs_kesehatan,
        bpjs_ketenagakerjaan: b.bpjs_ketenagakerjaan,
        penalty_deduction: b.penalty_deduction,
        late_deduction: b.late_deduction,
        absence_deduction: b.absence_deduction,
        potongan_lain: b.potongan_lain,
        other_deduction: b.other_deduction,
        context: Object.assign({}, b.context || {}, {
          worked_days: b.worked_days,
          total_work_days: b.total_work_days,
          overtime_hours: b.overtime_hours,
          rate_per_hour: b.rate_per_hour,
          present_days: b.present_days,
          total_days: b.total_days,
          base_allowance: b.base_allowance,
          tax_rate: b.tax_rate,
          late_minutes: b.late_minutes,
          penalty_rate: b.penalty_rate,
          absent_days: b.absent_days,
          daily_salary: b.daily_salary,
          apply_bpjs_kesehatan: b.apply_bpjs_kesehatan,
          apply_bpjs_ketenagakerjaan: b.apply_bpjs_ketenagakerjaan,
          apply_tax: b.apply_tax,
          UMP_JAM: b.UMP_JAM || b.ump_jam || b.gaji_pokok || b.basic_salary || b.full_salary,
          MAX_KS: b.MAX_KS || b.max_ks,
          MAX_JP: b.MAX_JP || b.max_jp
        })
      });
      if (Array.isArray(payrollResult.errors) && payrollResult.errors.length > 0) return json(res, 400, { ok: false, message: payrollResult.errors[0], errors: payrollResult.errors });
      const payload = {
        doc_id: rid('PAY'),
        employee_id: String(b.employee_id || '').trim(),
        email: String(b.email || '').trim().toLowerCase(),
        bulan: String(b.bulan || defaultBulan).trim(),
        tahun: String(b.tahun || defaultTahun).trim(),
        nama_file: String(b.nama_file || autoFileName).trim(),
        file_url: String(b.file_url || '').trim(),
        keterangan: composePayrollKeterangan(String(b.keterangan || '').trim(), {
          version: 2,
          employee_id: employeeId,
          context: Object.assign({}, b.context || {}, {
            UMP_JAM: b.UMP_JAM || b.ump_jam || b.gaji_pokok || b.basic_salary || b.full_salary,
            MAX_KS: b.MAX_KS || b.max_ks,
            MAX_JP: b.MAX_JP || b.max_jp
          }),
          components: payrollResult.breakdown.earnings.concat(payrollResult.breakdown.deductions),
          payroll_output: payrollResult
        }),
        uploaded_at: b.uploaded_at || nowIso()
      };
      payload.email = resolvedEmail;
      if (!payload.employee_id || !payload.nama_file || !payload.email) return json(res, 400, { ok: false, message: 'employee_id, email, dan nama_file wajib diisi.' });
      if (payload.bulan && payload.tahun) {
        const dup = await db('GET', 'payroll_docs', { select: 'doc_id,uploaded_at', employee_id: 'eq.' + payload.employee_id, bulan: 'eq.' + payload.bulan, tahun: 'eq.' + payload.tahun, limit: 3 });
        if (!dup.ok) return json(res, 500, { ok: false, message: 'Gagal validasi duplikasi payroll.', error: dup.error });
        if (Array.isArray(dup.data) && dup.data.length > 0) return json(res, 409, { ok: false, message: 'Payroll untuk periode ini sudah ada pada karyawan tersebut.' });
      }
      const ins = await db('POST', 'payroll_docs', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal membuat payroll doc.', error: ins.error });
      const inserted = Array.isArray(ins.data) && ins.data[0] ? ins.data[0] : null;
      if (inserted) {
        const enriched = enrichPayrollDoc(inserted);
        const pdf = payrollPdfBuffer(enriched, employeeName || employeeId);
        const pdfName = toSafeFileToken('payslip_' + String(payload.bulan || '') + '_' + String(payload.tahun || '') + '_' + String(employeeName || employeeId || ''), 'payslip') + '.pdf';
        const pdfUrl = await uploadBufferToDrive(pdfName, 'application/pdf', pdf, payrollDriveFolderId());
        if (!pdfUrl) {
          await db('DELETE', 'payroll_docs', { doc_id: 'eq.' + String(inserted.doc_id || payload.doc_id) });
          return json(res, 500, { ok: false, message: 'Gagal generate/upload payslip PDF ke Google Drive. Data payroll dibatalkan agar konsisten.' });
        }
        const patchDoc = await db('PATCH', 'payroll_docs', { doc_id: 'eq.' + String(inserted.doc_id || payload.doc_id) }, { file_url: pdfUrl }, { Prefer: 'return=representation' });
        if (!patchDoc.ok) {
          await db('DELETE', 'payroll_docs', { doc_id: 'eq.' + String(inserted.doc_id || payload.doc_id) });
          return json(res, 500, { ok: false, message: 'PDF berhasil dibuat tetapi gagal simpan URL ke payroll. Data payroll dibatalkan agar konsisten.', error: patchDoc.error });
        }
      }
      await auditLog(a.email, 'CREATE', 'payroll_docs', 'Tambah payroll doc ' + payload.doc_id + ' untuk ' + payload.employee_id, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      const out = await db('GET', 'payroll_docs', { select: '*', doc_id: 'eq.' + String(payload.doc_id), limit: 1 });
      const data = out.ok ? (out.data || []) : (ins.data || []);
      return json(res, 200, { ok: true, message: 'Payroll doc berhasil dibuat.', data: data.map(enrichPayrollDoc) });
    }

    if (path === 'admin/payroll/import-csv' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      let csvText = String(b.csv_text || '').trim();
      if (!csvText && b.csv_base64) {
        try { csvText = Buffer.from(String(b.csv_base64 || ''), 'base64').toString('utf8'); } catch (_) { csvText = ''; }
      }
      if (!csvText) return json(res, 400, { ok: false, message: 'CSV kosong.' });
      const rows = csvRowsToObjects(csvText);
      if (!rows.length) return json(res, 400, { ok: false, message: 'CSV tidak memiliki data baris.' });
      const headers = Object.keys(rows[0] || {});
      const normalize = function(s) { return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim(); };
      const hmap = {};
      headers.forEach(function(h) { hmap[normalize(h)] = h; });
      const need = [
        'employee id number', 'full name', 'ktp number', 'basic salary', 'bonus',
        'transportation allowance', 'role based allowance', 'thr',
        'bpjs kesehatan perusahaan 4 deduction', 'bpjs kesehatan karyawan 1 deduction',
        'jht 3 7 by company deduction', 'jht 2 by employee deduction',
        'jaminan pensiun 2 perusahaan deduction', 'jaminan pensiun 1 karyawan deduction',
        'jkk 0 24 deduction', 'jkm 0 3 deduction', 'tax', 'take home pay'
      ];
      const missingHeader = need.filter(function(k) { return !hmap[k]; });
      if (missingHeader.length) return json(res, 400, { ok: false, message: 'Header CSV belum sesuai.', missing_headers: missingHeader });
      const now = new Date();
      const prev = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const defaultMonth = prev.toLocaleString('id-ID', { month: 'long' });
      const defaultYear = String(prev.getFullYear());
      const bulan = String(b.bulan || defaultMonth).trim();
      const tahun = String(b.tahun || defaultYear).trim();
      const parseNum = function(v) {
        const s = String(v || '').replace(/\s+/g, '').replace(/\./g, '').replace(/,/g, '.').replace(/[^0-9.\-]/g, '');
        const n = Number(s || 0);
        return Number.isFinite(n) ? n : 0;
      };
      const activeEmployees = await db('GET', 'employees', { select: 'employee_id,nama,email', limit: 5000 });
      if (!activeEmployees.ok) return json(res, 500, { ok: false, message: 'Gagal ambil data karyawan.', error: activeEmployees.error });
      const empMap = {};
      (activeEmployees.data || []).forEach(function(e) { empMap[String(e.employee_id || '').trim()] = e; });
      const result = { period: { bulan: bulan, tahun: tahun }, total_rows: rows.length, inserted: 0, updated: 0, failed: 0, details: [] };
      for (const row of rows) {
        const employeeId = String(row[hmap['employee id number']] || '').trim();
        const fullName = String(row[hmap['full name']] || '').trim();
        const ktp = String(row[hmap['ktp number']] || '').trim();
        if (!employeeId) { result.failed += 1; result.details.push({ employee_id: '', status: 'failed', reason: 'employee_id_kosong' }); continue; }
        const emp = empMap[employeeId] || null;
        const email = String((emp && emp.email) || '').trim().toLowerCase();
        if (!isValidEmail(email)) { result.failed += 1; result.details.push({ employee_id: employeeId, status: 'failed', reason: 'email_karyawan_tidak_valid' }); continue; }
        const components = [
          { name: 'Basic Salary', type: 'EARNING', category: 'FIXED', value: parseNum(row[hmap['basic salary']]) },
          { name: 'Bonus / Incentive', type: 'EARNING', category: 'VARIABLE', value: parseNum(row[hmap['bonus']]) },
          { name: 'Transport Allowance', type: 'EARNING', category: 'FIXED', value: parseNum(row[hmap['transportation allowance']]) },
          { name: 'Allowance', type: 'EARNING', category: 'FIXED', value: parseNum(row[hmap['role based allowance']]) },
          { name: 'THR', type: 'EARNING', category: 'VARIABLE', value: parseNum(row[hmap['thr']]) },
          { name: 'BPJS KESEHATAN PERUSAHAAN 4% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['bpjs kesehatan perusahaan 4 deduction']]) },
          { name: 'BPJS KESEHATAN KARYAWAN 1% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['bpjs kesehatan karyawan 1 deduction']]) },
          { name: 'JHT 3.7% BY COMPANY DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['jht 3 7 by company deduction']]) },
          { name: 'JHT 2% BY EMPLOYEE DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['jht 2 by employee deduction']]) },
          { name: 'JAMINAN PENSIUN 2% PERUSAHAAN DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['jaminan pensiun 2 perusahaan deduction']]) },
          { name: 'JAMINAN PENSIUN 1% KARYAWAN DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['jaminan pensiun 1 karyawan deduction']]) },
          { name: 'JKK 0.24% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['jkk 0 24 deduction']]) },
          { name: 'JKM 0.3% DEDUCTION', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['jkm 0 3 deduction']]) },
          { name: 'Tax (PPh21)', type: 'DEDUCTION', category: 'VARIABLE', value: parseNum(row[hmap['tax']]) }
        ];
        const thpCsv = parseNum(row[hmap['take home pay']]);
        const calc = payrollEngine({ employee_id: employeeId, components: components, context: {} });
        if (Array.isArray(calc.errors) && calc.errors.length) { result.failed += 1; result.details.push({ employee_id: employeeId, status: 'failed', reason: calc.errors[0] }); continue; }
        let payrollOut = calc;
        const diff = toMoney(thpCsv - Number(calc.net_salary || 0));
        if (Math.abs(diff) >= 1) {
          const adjComp = components.concat([{ name: 'CSV Adjustment', type: diff > 0 ? 'EARNING' : 'DEDUCTION', category: 'VARIABLE', value: Math.abs(diff) }]);
          payrollOut = payrollEngine({ employee_id: employeeId, components: adjComp, context: {} });
        }
        const payload = {
          doc_id: rid('PAY'),
          employee_id: employeeId,
          email: email,
          bulan: bulan,
          tahun: tahun,
          nama_file: 'Slip Gaji ' + bulan + ' ' + tahun + ' - ' + String((emp && emp.nama) || fullName || employeeId),
          file_url: '',
          keterangan: composePayrollKeterangan('Import CSV payroll', {
            version: 3,
            employee_id: employeeId,
            csv_source: { full_name: fullName, ktp_number: ktp, take_home_pay_csv: thpCsv },
            components: payrollOut.breakdown.earnings.concat(payrollOut.breakdown.deductions),
            payroll_output: payrollOut
          }),
          uploaded_at: nowIso()
        };
        const dup = await db('GET', 'payroll_docs', { select: '*', employee_id: 'eq.' + employeeId, bulan: 'eq.' + bulan, tahun: 'eq.' + tahun, limit: 1 });
        if (!dup.ok) { result.failed += 1; result.details.push({ employee_id: employeeId, status: 'failed', reason: 'gagal_cek_duplikasi' }); continue; }
        let stored = null;
        let status = 'inserted';
        if (Array.isArray(dup.data) && dup.data[0]) {
          const docId = String(dup.data[0].doc_id || '');
          const patch = await db('PATCH', 'payroll_docs', { doc_id: 'eq.' + docId }, {
            email: payload.email,
            nama_file: payload.nama_file,
            keterangan: payload.keterangan,
            uploaded_at: payload.uploaded_at
          }, { Prefer: 'return=representation' });
          if (!patch.ok || !Array.isArray(patch.data) || !patch.data[0]) { result.failed += 1; result.details.push({ employee_id: employeeId, status: 'failed', reason: 'gagal_update_doc' }); continue; }
          stored = patch.data[0];
          status = 'updated';
        } else {
          const ins = await db('POST', 'payroll_docs', null, payload, { Prefer: 'return=representation' });
          if (!ins.ok || !Array.isArray(ins.data) || !ins.data[0]) { result.failed += 1; result.details.push({ employee_id: employeeId, status: 'failed', reason: 'gagal_insert_doc' }); continue; }
          stored = ins.data[0];
          status = 'inserted';
        }
        const enriched = enrichPayrollDoc(stored);
        const pdf = payrollPdfBuffer(enriched, String((emp && emp.nama) || fullName || employeeId));
        const pdfName = toSafeFileToken('payslip_' + bulan + '_' + tahun + '_' + String((emp && emp.nama) || fullName || employeeId), 'payslip') + '.pdf';
        const pdfUrl = await uploadBufferToDrive(pdfName, 'application/pdf', pdf, payrollDriveFolderId());
        if (pdfUrl) await db('PATCH', 'payroll_docs', { doc_id: 'eq.' + String(stored.doc_id || '') }, { file_url: pdfUrl }, { Prefer: 'return=minimal' });
        if (status === 'inserted') result.inserted += 1; else result.updated += 1;
        result.details.push({ employee_id: employeeId, status: status, file_url: pdfUrl || '' });
      }
      await auditLog(a.email, 'IMPORT', 'payroll_docs', 'Import payroll CSV period ' + bulan + ' ' + tahun + ' rows=' + String(rows.length), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Import payroll CSV selesai.', result: result });
    }

    if (path === 'admin/payroll/calculate' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const period = {
        bulan: String((b.period && b.period.bulan) || b.bulan || '').trim(),
        tahun: String((b.period && b.period.tahun) || b.tahun || '').trim()
      };
      let objects = [];
      if (Array.isArray(b.rows) && b.rows.length > 0) objects = componentMapperFromMatrixRows(b.rows);
      else if (Array.isArray(b.payrolls) && b.payrolls.length > 0) objects = b.payrolls;
      else if (b.employee_id || Array.isArray(b.components)) objects = [b];
      else return json(res, 400, { ok: false, message: 'Input payroll tidak valid. Gunakan rows/payrolls/components.' });
      const contextByEmployee = (b.context_by_employee && typeof b.context_by_employee === 'object') ? b.context_by_employee : {};
      const outputs = objects.map(function(x) {
        const employeeId = String(x.employee_id || '').trim();
        const merged = Object.assign({}, x, { employee_id: employeeId, context: Object.assign({}, b.context || {}, x.context || {}, contextByEmployee[employeeId] || {}) });
        return payrollEngine(merged);
      });
      const cacheKey = 'payroll-calc:' + (period.bulan || '-') + ':' + (period.tahun || '-') + ':' + String(outputs.length);
      cacheSet(cacheKey, outputs, 5 * 60 * 1000);
      return json(res, 200, { ok: true, period: period, total_employees: outputs.length, outputs: outputs });
    }

    if (path === 'admin/payroll/export-pdf' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const docId = String(b.doc_id || '').trim();
      if (!docId) return json(res, 400, { ok: false, message: 'doc_id wajib diisi.' });
      const d = await db('GET', 'payroll_docs', { select: '*', doc_id: 'eq.' + docId, limit: 1 });
      if (!d.ok) return json(res, 500, { ok: false, message: 'Gagal ambil payroll doc.', error: d.error });
      const row = Array.isArray(d.data) && d.data[0] ? d.data[0] : null;
      if (!row) return json(res, 404, { ok: false, message: 'Payroll doc tidak ditemukan.' });
      const em = await db('GET', 'employees', { select: 'nama', employee_id: 'eq.' + String(row.employee_id || ''), limit: 1 });
      const employeeName = em.ok && Array.isArray(em.data) && em.data[0] ? String(em.data[0].nama || row.employee_id || 'Karyawan') : String(row.employee_id || 'Karyawan');
      const enriched = enrichPayrollDoc(row);
      const pdf = payrollPdfBuffer(enriched, employeeName);
      const pdfName = toSafeFileToken('payslip_' + String(row.bulan || '') + '_' + String(row.tahun || '') + '_' + employeeName, 'payslip') + '.pdf';
      const pdfUrl = await uploadBufferToDrive(pdfName, 'application/pdf', pdf, payrollDriveFolderId());
      if (!pdfUrl) return json(res, 500, { ok: false, message: 'Gagal upload PDF payslip ke Google Drive.' });
      const upd = await db('PATCH', 'payroll_docs', { doc_id: 'eq.' + docId }, { file_url: pdfUrl }, { Prefer: 'return=representation' });
      if (!upd.ok) return json(res, 500, { ok: false, message: 'PDF berhasil dibuat, tetapi gagal update payroll doc.', file_url: pdfUrl, error: upd.error });
      await auditLog(a.email, 'EXPORT', 'payroll_docs', 'Export payslip PDF ' + docId, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Payslip PDF berhasil dibuat dan disimpan ke Google Drive.', file_url: pdfUrl, data: (upd.data || []).map(enrichPayrollDoc) });
    }

    if (path === 'admin/payroll/summary' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const bulan = String(req.query.bulan || '').trim();
      const tahun = String(req.query.tahun || '').trim();
      const summaryCacheKey = 'payroll-summary:' + (bulan || '-') + ':' + (tahun || '-');
      const cachedSummary = cacheGet(summaryCacheKey);
      if (cachedSummary) return json(res, 200, cachedSummary);
      const [emps, docs] = await Promise.all([
        db('GET', 'employees', { select: 'employee_id,nama,email,divisi,is_active', order: 'employee_id.asc', limit: 5000 }),
        db('GET', 'payroll_docs', { select: 'doc_id,employee_id,email,bulan,tahun,uploaded_at,nama_file,keterangan', order: 'uploaded_at.desc', limit: 5000 })
      ]);
      if (!emps.ok || !docs.ok) return json(res, 500, { ok: false, message: 'Gagal ambil payroll summary.', error: (!emps.ok ? emps.error : docs.error) });
      const active = (emps.data || []).filter(function(e) { return String(e.is_active).toLowerCase() === 'true'; });
      const filteredDocs = (docs.data || []).map(enrichPayrollDoc).filter(function(d) {
        if (bulan && String(d.bulan || '') !== bulan) return false;
        if (tahun && String(d.tahun || '') !== tahun) return false;
        return true;
      });
      const byEmp = {};
      filteredDocs.forEach(function(d) {
        const id = String(d.employee_id || '');
        if (!byEmp[id]) byEmp[id] = [];
        byEmp[id].push(d);
      });
      const coveredEmployeeIds = Object.keys(byEmp);
      const duplicates = coveredEmployeeIds.filter(function(id) { return (byEmp[id] || []).length > 1; });
      const missing = active.filter(function(e) { return !byEmp[String(e.employee_id || '')]; }).map(function(e) {
        return { employee_id: e.employee_id, nama: e.nama, email: e.email, divisi: e.divisi };
      });
      const divisionStats = {};
      active.forEach(function(e) {
        const div = String(e.divisi || 'Tanpa Divisi');
        if (!divisionStats[div]) divisionStats[div] = { divisi: div, active_employees: 0, payroll_uploaded: 0, payroll_missing: 0 };
        divisionStats[div].active_employees += 1;
        if (byEmp[String(e.employee_id || '')]) divisionStats[div].payroll_uploaded += 1;
        else divisionStats[div].payroll_missing += 1;
      });
      const payload = {
        ok: true,
        period: { bulan: bulan || null, tahun: tahun || null },
        summary: {
          active_employees: active.length,
          payroll_docs: filteredDocs.length,
          covered_employees: coveredEmployeeIds.length,
          missing_employees: missing.length,
          duplicate_employees: duplicates.length,
          completion_rate: active.length > 0 ? Math.round((coveredEmployeeIds.length / active.length) * 10000) / 100 : 0,
          total_take_home_pay: toMoney(filteredDocs.reduce(function(acc, x) { return acc + Number(x.net_salary || x.take_home_pay || 0); }, 0))
        },
        duplicate_employee_ids: duplicates,
        missing_employees: missing.slice(0, 200),
        division_stats: Object.values(divisionStats).sort(function(a1, b1) { return Number(b1.payroll_missing || 0) - Number(a1.payroll_missing || 0); })
      };
      cacheSet(summaryCacheKey, payload, 5 * 60 * 1000);
      return json(res, 200, payload);
    }

    if (path === 'admin/master/divisions' || path === 'admin/master/positions' || path === 'admin/master/leave-types') {
      const a = requireAdmin(req, res); if (!a) return;
      if (path === 'admin/master/divisions' || path === 'admin/master/positions') {
        await syncDivisionPositionFromEmployees(a.email);
      }
      const map = {
        'admin/master/divisions': { table: 'divisions', id: 'division_id', name: 'nama_divisi' },
        'admin/master/positions': { table: 'positions', id: 'position_id', name: 'nama_jabatan' },
        'admin/master/leave-types': { table: 'leave_types', id: 'leave_type_id', name: 'nama_jenis_cuti' }
      };
      const meta = map[path];
      if (method === 'GET') {
        const r = await db('GET', meta.table, { select: '*', order: meta.name + '.asc' });
        if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil master data.', error: r.error });
        return json(res, 200, r.data || []);
      }
      if (method === 'POST') {
        const b = await readBody(req);
        const payload = Object.assign({}, b, { [meta.id]: String(b[meta.id] || rid(meta.id.toUpperCase())).trim(), created_at: nowIso(), updated_at: nowIso() });
        const ins = await db('POST', meta.table, null, payload, { Prefer: 'return=representation' });
        if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal membuat master data.', error: ins.error });
        return json(res, 200, { ok: true, data: ins.data });
      }
      if (method === 'PATCH') {
        const b = await readBody(req);
        const id = String(b[meta.id] || '').trim();
        if (!id) return json(res, 400, { ok: false, message: meta.id + ' wajib diisi.' });
        const patch = await db('PATCH', meta.table, { [meta.id]: 'eq.' + id }, Object.assign({}, b, { updated_at: nowIso() }), { Prefer: 'return=representation' });
        if (!patch.ok) return json(res, 500, { ok: false, message: 'Gagal update master data.', error: patch.error });
        return json(res, 200, { ok: true, data: patch.data });
      }
      return json(res, 405, { ok: false, message: 'Method tidak didukung.' });
    }

    if (path === 'admin/notifications/leave/summary' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const p = await db('GET', 'leave_requests', { select: 'leave_id', status: 'eq.pending' });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal ambil summary notifikasi leave.', error: p.error });
      const count = Array.isArray(p.data) ? p.data.length : 0;
      return json(res, 200, { unread_leave_requests: count, total_unread: count });
    }

    if (path === 'admin/notifications/leave' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const p = await db('GET', 'leave_requests', { select: 'leave_id,employee_id,email,jenis_cuti,tanggal_mulai,tanggal_selesai,status,created_at', status: 'eq.pending', order: 'created_at.desc', limit: Math.min(Number(req.query.limit || 100), 500) });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal ambil notifikasi leave.', error: p.error });
      const rows = (p.data || []).map(function(r) {
        return { notification_type: 'leave_request', item_id: r.leave_id || '', title: 'Pengajuan Cuti Baru', message: (r.email || '-') + ' mengajukan ' + (r.jenis_cuti || '-') + ' (' + (r.tanggal_mulai || '-') + ' s/d ' + (r.tanggal_selesai || '-') + ')', date_value: r.created_at || '', is_unread: 'TRUE' };
      });
      return json(res, 200, rows);
    }

    if (path === 'admin/notifications/leave/mark-seen' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      return json(res, 200, { ok: true, message: 'Notifikasi leave ditandai dilihat.' });
    }

    if (path === 'admin/incidents/timeline' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const limit = Math.min(Number(req.query.limit || 200), 500);
      const [leaves, audits, attendance, announcements] = await Promise.all([
        db('GET', 'leave_requests', { select: 'leave_id,email,jenis_cuti,status,created_at,updated_at', order: 'created_at.desc', limit: 120 }),
        db('GET', 'audit_log', { select: 'timestamp,user_email,aksi,modul,detail', order: 'timestamp.desc', limit: 150 }),
        db('GET', 'attendance', { select: 'attendance_id,employee_id,status,jam_masuk,tanggal,created_at', order: 'created_at.desc', limit: 150 }),
        db('GET', 'announcements', { select: 'announcement_id,judul,published_at,target_role,is_active,created_by', order: 'published_at.desc', limit: 100 })
      ]);
      if (!leaves.ok || !audits.ok || !attendance.ok || !announcements.ok) return json(res, 500, { ok: false, message: 'Gagal ambil incident timeline.', error: (!leaves.ok ? leaves.error : (!audits.ok ? audits.error : (!attendance.ok ? attendance.error : announcements.error))) });
      const timeline = [];
      (leaves.data || []).forEach(function(x) {
        const st = String(x.status || '').toLowerCase();
        timeline.push({
          category: 'leave',
          severity: st === 'pending' ? 'medium' : (st === 'rejected' ? 'high' : 'info'),
          title: 'Leave ' + String(x.status || '-'),
          detail: String(x.email || '-') + ' • ' + String(x.jenis_cuti || '-'),
          occurred_at: x.updated_at || x.created_at || '',
          source_ref: String(x.leave_id || ''),
          action_route: 'leave'
        });
      });
      (audits.data || []).forEach(function(x) {
        const action = String(x.aksi || '').toUpperCase();
        timeline.push({
          category: 'audit',
          severity: action.indexOf('REJECT') >= 0 ? 'high' : (action.indexOf('APPROVE') >= 0 ? 'medium' : 'info'),
          title: 'Audit ' + action,
          detail: String(x.user_email || '-') + ' • ' + String(x.modul || '-') + ' • ' + String(x.detail || ''),
          occurred_at: x.timestamp || '',
          source_ref: '',
          action_route: 'ops'
        });
      });
      (attendance.data || []).forEach(function(x) {
        const st = String(x.status || '').toLowerCase();
        if (st !== 'terlambat') return;
        timeline.push({
          category: 'attendance',
          severity: 'high',
          title: 'Terlambat Tercatat',
          detail: String(x.employee_id || '-') + ' • ' + String(x.tanggal || '-') + ' • masuk ' + String(x.jam_masuk || '-'),
          occurred_at: x.created_at || '',
          source_ref: String(x.attendance_id || ''),
          action_route: 'att'
        });
      });
      (announcements.data || []).forEach(function(x) {
        timeline.push({
          category: 'announcement',
          severity: String(x.is_active).toLowerCase() === 'true' ? 'info' : 'medium',
          title: 'Announcement Published',
          detail: String(x.judul || '-') + ' • target ' + String(x.target_role || 'all'),
          occurred_at: x.published_at || '',
          source_ref: String(x.announcement_id || ''),
          action_route: 'master'
        });
      });
      timeline.sort(function(a1, b1) { return new Date(b1.occurred_at || 0).getTime() - new Date(a1.occurred_at || 0).getTime(); });
      return json(res, 200, { ok: true, timeline: timeline.slice(0, limit) });
    }

    if (path === 'admin/drive/status' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const folderId = driveFolderId();
      const tk = await getDriveAccessToken();
      if (!folderId) return json(res, 200, { ok: true, drive_ready: false, reason: 'ATTENDANCE_DRIVE_FOLDER tidak valid.' });
      if (!tk.ok) return json(res, 200, { ok: true, drive_ready: false, folder_id: folderId, reason: tk.error });
      const r = await fetch('https://www.googleapis.com/drive/v3/files/' + folderId + '?fields=id,name', {
        method: 'GET',
        headers: { Authorization: 'Bearer ' + tk.token }
      });
      const tx = await r.text();
      let j = null;
      try { j = tx ? JSON.parse(tx) : null; } catch (e) { j = null; }
      if (!r.ok) return json(res, 200, { ok: true, drive_ready: false, folder_id: folderId, token_source: tk.source, reason: j || tx || 'Tidak bisa akses folder Drive.' });
      return json(res, 200, { ok: true, drive_ready: true, folder_id: folderId, folder_name: (j && j.name) || '', token_source: tk.source });
    }

    if (path === 'admin/kpi/hr' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const today = ymd();
      const startDate = String(req.query.start_date || dateShift(today, -29)).trim();
      const endDate = String(req.query.end_date || today).trim();
      const periodDays = dateRangeList(startDate, endDate);
      if (!periodDays.length) return json(res, 400, { ok: false, message: 'Periode tidak valid.' });
      const attendanceQ = { select: 'employee_id,email,tanggal,jam_masuk,status,work_mode', order: 'tanggal.asc,created_at.asc', limit: Math.min(Number(req.query.limit || 20000), 30000), and: '(tanggal.gte.' + startDate + ',tanggal.lte.' + endDate + ')' };
      const leaveQ = { select: 'employee_id,email,jenis_cuti,tanggal_mulai,status,created_at', order: 'tanggal_mulai.asc,created_at.asc', limit: Math.min(Number(req.query.limit || 20000), 30000), and: '(tanggal_mulai.gte.' + startDate + ',tanggal_mulai.lte.' + endDate + ')' };
      const rAttendance = await db('GET', 'attendance', attendanceQ);
      const rLeaves = await db('GET', 'leave_requests', leaveQ);
      const rEmp = await db('GET', 'employees', { select: 'employee_id,nama,divisi,jabatan,is_active', order: 'employee_id.asc', limit: 5000 });
      const rCfg = await db('GET', 'config', { select: 'key,value', key: 'eq.LATE_AFTER_TIME', limit: 1 });
      if (!rAttendance.ok || !rLeaves.ok || !rEmp.ok || !rCfg.ok) {
        const err = !rAttendance.ok ? rAttendance.error : !rLeaves.ok ? rLeaves.error : !rEmp.ok ? rEmp.error : rCfg.error;
        return json(res, 500, { ok: false, message: 'Gagal ambil data KPI HR.', error: err });
      }
      const attendanceRows = rAttendance.data || [];
      const leaveRows = rLeaves.data || [];
      const empRows = rEmp.data || [];
      const lateAfter = String((rCfg.data && rCfg.data[0] && rCfg.data[0].value) || '08:30:00');
      const empMap = {};
      empRows.forEach(function(x) { empMap[String(x.employee_id || '')] = x; });
      const trendAttendance = {};
      const trendLeaves = {};
      periodDays.forEach(function(d) {
        trendAttendance[d] = { date: d, total: 0, hadir: 0, terlambat: 0, wfh: 0, office: 0 };
        trendLeaves[d] = { date: d, total: 0, pending: 0, approved: 0, rejected: 0 };
      });
      const lateMap = {};
      const punctualMap = {};
      const divisionLateMap = {};
      const divisionDateMap = {};
      let lateCount = 0;
      attendanceRows.forEach(function(r) {
        const d = String(r.tanggal || '');
        if (!trendAttendance[d]) return;
        trendAttendance[d].total += 1;
        const st = String(r.status || '').toLowerCase();
        if (st === 'hadir') trendAttendance[d].hadir += 1;
        if (String(r.work_mode || '').toLowerCase() === 'wfh') trendAttendance[d].wfh += 1;
        if (String(r.work_mode || '').toLowerCase() === 'office') trendAttendance[d].office += 1;
        const jamMasuk = String(r.jam_masuk || '');
        const isLate = st === 'terlambat' || (jamMasuk && jamMasuk > lateAfter);
        if (isLate) {
          trendAttendance[d].terlambat += 1;
          lateCount += 1;
          const key = String(r.employee_id || '');
          if (!lateMap[key]) lateMap[key] = { employee_id: key, email: String(r.email || ''), nama: '', divisi: '', jabatan: '', late_count: 0 };
          lateMap[key].late_count += 1;
        }
        const keyP = String(r.employee_id || '');
        if (!punctualMap[keyP]) punctualMap[keyP] = { employee_id: keyP, email: String(r.email || ''), nama: '', divisi: '', jabatan: '', present_count: 0, ontime_count: 0, late_count: 0, punctual_rate: 0 };
        punctualMap[keyP].present_count += 1;
        if (isLate) punctualMap[keyP].late_count += 1;
        else punctualMap[keyP].ontime_count += 1;
        const emp = empMap[keyP] || {};
        const divKey = String(emp.divisi || 'Tanpa Divisi');
        if (!divisionLateMap[divKey]) divisionLateMap[divKey] = { divisi: divKey, attendance_records: 0, late_records: 0, ontime_records: 0, late_rate: 0 };
        divisionLateMap[divKey].attendance_records += 1;
        if (isLate) divisionLateMap[divKey].late_records += 1;
        else divisionLateMap[divKey].ontime_records += 1;
        const ddKey = divKey + '|' + d;
        if (!divisionDateMap[ddKey]) divisionDateMap[ddKey] = { divisi: divKey, date: d, attendance_records: 0, late_records: 0, ontime_records: 0, late_rate: 0 };
        divisionDateMap[ddKey].attendance_records += 1;
        if (isLate) divisionDateMap[ddKey].late_records += 1;
        else divisionDateMap[ddKey].ontime_records += 1;
      });
      leaveRows.forEach(function(r) {
        const d = String(r.tanggal_mulai || '');
        if (!trendLeaves[d]) return;
        trendLeaves[d].total += 1;
        const st = String(r.status || '').toLowerCase();
        if (st === 'pending') trendLeaves[d].pending += 1;
        if (st === 'approved') trendLeaves[d].approved += 1;
        if (st === 'rejected') trendLeaves[d].rejected += 1;
      });
      Object.keys(lateMap).forEach(function(k) {
        const e = empMap[k] || {};
        lateMap[k].nama = String(e.nama || '-');
        lateMap[k].divisi = String(e.divisi || '-');
        lateMap[k].jabatan = String(e.jabatan || '-');
      });
      const topLate = Object.values(lateMap).sort(function(a1, b1) { return Number(b1.late_count || 0) - Number(a1.late_count || 0); }).slice(0, 10);
      Object.keys(punctualMap).forEach(function(k) {
        const e = empMap[k] || {};
        punctualMap[k].nama = String(e.nama || '-');
        punctualMap[k].divisi = String(e.divisi || '-');
        punctualMap[k].jabatan = String(e.jabatan || '-');
        const present = Number(punctualMap[k].present_count || 0);
        const ontime = Number(punctualMap[k].ontime_count || 0);
        punctualMap[k].punctual_rate = present > 0 ? Math.round((ontime / present) * 10000) / 100 : 0;
      });
      const topPunctual = Object.values(punctualMap)
        .filter(function(x) { return Number(x.present_count || 0) > 0; })
        .sort(function(a1, b1) {
          if (Number(b1.punctual_rate || 0) !== Number(a1.punctual_rate || 0)) return Number(b1.punctual_rate || 0) - Number(a1.punctual_rate || 0);
          if (Number(b1.ontime_count || 0) !== Number(a1.ontime_count || 0)) return Number(b1.ontime_count || 0) - Number(a1.ontime_count || 0);
          return Number(b1.present_count || 0) - Number(a1.present_count || 0);
        })
        .slice(0, 10);
      const lateByDivision = Object.values(divisionLateMap).map(function(x) {
        const total = Number(x.attendance_records || 0);
        const late = Number(x.late_records || 0);
        x.late_rate = total > 0 ? Math.round((late / total) * 10000) / 100 : 0;
        return x;
      }).sort(function(a1, b1) { return Number(b1.late_rate || 0) - Number(a1.late_rate || 0); });
      const lateHeatmap = Object.values(divisionDateMap).map(function(x) {
        const total = Number(x.attendance_records || 0);
        const late = Number(x.late_records || 0);
        x.late_rate = total > 0 ? Math.round((late / total) * 10000) / 100 : 0;
        return x;
      }).sort(function(a1, b1) {
        if (String(a1.divisi || '') !== String(b1.divisi || '')) return String(a1.divisi || '').localeCompare(String(b1.divisi || ''));
        return String(a1.date || '').localeCompare(String(b1.date || ''));
      });
      const summary = {
        start_date: startDate,
        end_date: endDate,
        period_days: periodDays.length,
        total_employees: empRows.length,
        active_employees: empRows.filter(function(x) { return String(x.is_active).toLowerCase() === 'true'; }).length,
        attendance_records: attendanceRows.length,
        late_records: lateCount,
        leave_requests: leaveRows.length,
        pending_leaves: leaveRows.filter(function(x) { return String(x.status || '').toLowerCase() === 'pending'; }).length,
        late_after_time: lateAfter
      };
      await auditLog(a.email, 'REPORT', 'kpi_hr', 'Generate KPI HR ' + startDate + ' s/d ' + endDate, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, {
        ok: true,
        summary: summary,
        trend_attendance: periodDays.map(function(d) { return trendAttendance[d]; }),
        trend_leaves: periodDays.map(function(d) { return trendLeaves[d]; }),
        top_late: topLate,
        top_punctual: topPunctual,
        late_by_division: lateByDivision,
        late_heatmap: lateHeatmap
      });
    }

    if (path === 'admin/kpi/hr/day' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const date = String(req.query.date || '').trim();
      if (!date) return json(res, 400, { ok: false, message: 'date wajib diisi (YYYY-MM-DD).' });
      const rCfg = await db('GET', 'config', { select: 'key,value', key: 'eq.LATE_AFTER_TIME', limit: 1 });
      const rEmp = await db('GET', 'employees', { select: 'employee_id,nama,divisi,jabatan', order: 'employee_id.asc', limit: 5000 });
      const rAttendance = await db('GET', 'attendance', { select: 'attendance_id,employee_id,email,tanggal,jam_masuk,jam_keluar,status,lokasi,work_mode', tanggal: 'eq.' + date, order: 'jam_masuk.asc,created_at.asc', limit: 5000 });
      const rLeaves = await db('GET', 'leave_requests', { select: 'leave_id,employee_id,email,jenis_cuti,tanggal_mulai,tanggal_selesai,status,created_at', tanggal_mulai: 'eq.' + date, order: 'created_at.desc', limit: 5000 });
      if (!rCfg.ok || !rEmp.ok || !rAttendance.ok || !rLeaves.ok) {
        const err = !rCfg.ok ? rCfg.error : !rEmp.ok ? rEmp.error : !rAttendance.ok ? rAttendance.error : rLeaves.error;
        return json(res, 500, { ok: false, message: 'Gagal ambil detail KPI harian.', error: err });
      }
      const lateAfter = String((rCfg.data && rCfg.data[0] && rCfg.data[0].value) || '08:30:00');
      const empMap = {};
      (rEmp.data || []).forEach(function(x) { empMap[String(x.employee_id || '')] = x; });
      const attendance = (rAttendance.data || []).map(function(r) {
        const e = empMap[String(r.employee_id || '')] || {};
        return Object.assign({}, r, { nama: String(e.nama || '-'), divisi: String(e.divisi || '-'), jabatan: String(e.jabatan || '-') });
      });
      const leaves = (rLeaves.data || []).map(function(r) {
        const e = empMap[String(r.employee_id || '')] || {};
        return Object.assign({}, r, { nama: String(e.nama || '-'), divisi: String(e.divisi || '-'), jabatan: String(e.jabatan || '-') });
      });
      const lateRows = attendance.filter(function(r) {
        const st = String(r.status || '').toLowerCase();
        const jm = String(r.jam_masuk || '');
        return st === 'terlambat' || (jm && jm > lateAfter);
      }).sort(function(x, y) {
        return String(y.jam_masuk || '').localeCompare(String(x.jam_masuk || ''));
      });
      const summary = {
        date: date,
        attendance_total: attendance.length,
        attendance_hadir: attendance.filter(function(r) { return String(r.status || '').toLowerCase() === 'hadir'; }).length,
        attendance_terlambat: lateRows.length,
        leaves_total: leaves.length,
        leaves_pending: leaves.filter(function(r) { return String(r.status || '').toLowerCase() === 'pending'; }).length,
        leaves_approved: leaves.filter(function(r) { return String(r.status || '').toLowerCase() === 'approved'; }).length,
        leaves_rejected: leaves.filter(function(r) { return String(r.status || '').toLowerCase() === 'rejected'; }).length,
        late_after_time: lateAfter
      };
      await auditLog(a.email, 'REPORT', 'kpi_hr_day', 'Open KPI harian ' + date, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, summary: summary, attendance: attendance, leaves: leaves, late_rows: lateRows });
    }

    if (path === 'admin/operations-intelligence/rules' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const keys = ['OPS_CHECKIN_GAP_HIGH', 'OPS_CHECKIN_GAP_CRITICAL', 'OPS_PENDING_LEAVES_MEDIUM', 'OPS_PENDING_LEAVES_CRITICAL', 'OPS_LATE_RATE_HIGH', 'OPS_DIVISION_RISK_MEDIUM'];
      const r = await db('GET', 'config', { select: 'key,value', key: 'in.(' + keys.join(',') + ')', limit: 100 });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil rules operations.', error: r.error });
      const map = {};
      (r.data || []).forEach(function(x) { map[String(x.key || '')] = String(x.value || ''); });
      return json(res, 200, {
        ok: true,
        rules: {
          checkin_gap_high: Number(map.OPS_CHECKIN_GAP_HIGH || 10),
          checkin_gap_critical: Number(map.OPS_CHECKIN_GAP_CRITICAL || 25),
          pending_leaves_medium: Number(map.OPS_PENDING_LEAVES_MEDIUM || 5),
          pending_leaves_critical: Number(map.OPS_PENDING_LEAVES_CRITICAL || 15),
          late_rate_high: Number(map.OPS_LATE_RATE_HIGH || 20),
          division_risk_medium: Number(map.OPS_DIVISION_RISK_MEDIUM || 20)
        }
      });
    }

    if (path === 'admin/operations-intelligence/rules' && method === 'PATCH') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const pairs = [
        { key: 'OPS_CHECKIN_GAP_HIGH', v: Number(b.checkin_gap_high) },
        { key: 'OPS_CHECKIN_GAP_CRITICAL', v: Number(b.checkin_gap_critical) },
        { key: 'OPS_PENDING_LEAVES_MEDIUM', v: Number(b.pending_leaves_medium) },
        { key: 'OPS_PENDING_LEAVES_CRITICAL', v: Number(b.pending_leaves_critical) },
        { key: 'OPS_LATE_RATE_HIGH', v: Number(b.late_rate_high) },
        { key: 'OPS_DIVISION_RISK_MEDIUM', v: Number(b.division_risk_medium) }
      ].filter(function(x) { return Number.isFinite(x.v); });
      if (!pairs.length) return json(res, 400, { ok: false, message: 'Tidak ada rule yang diupdate.' });
      for (const p of pairs) {
        const n = Math.max(0, Math.min(1000, Number(p.v)));
        const up = await db('PATCH', 'config', { key: 'eq.' + p.key }, { value: String(n), updated_at: nowIso() }, { Prefer: 'return=representation' });
        if (up.ok && Array.isArray(up.data) && up.data.length > 0) continue;
        const ins = await db('POST', 'config', null, { key: p.key, value: String(n), updated_at: nowIso() }, { Prefer: 'return=representation' });
        if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal simpan rule ' + p.key + '.', error: ins.error });
      }
      await auditLog(a.email, 'UPDATE', 'operations_rules', 'Update operations intelligence rules', String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Rules operations intelligence berhasil diperbarui.' });
    }

    if (path === 'admin/operations-intelligence/announce-action' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const title = String(b.title || '').trim();
      const detail = String(b.detail || '').trim();
      const targetRole = String(b.target_role || 'employee').trim().toLowerCase();
      if (!title || !detail) return json(res, 400, { ok: false, message: 'title dan detail wajib diisi.' });
      if (!['all', 'employee', 'admin'].includes(targetRole)) return json(res, 400, { ok: false, message: 'target_role tidak valid.' });
      const payload = {
        announcement_id: rid('ANN'),
        judul: '[OPS] ' + title,
        isi: detail + '\n\nSumber: Operations Intelligence',
        target_role: targetRole,
        published_at: nowIso(),
        expired_at: null,
        is_active: true,
        created_by: a.email
      };
      const ins = await db('POST', 'announcements', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal membuat auto-announcement.', error: ins.error });
      await auditLog(a.email, 'CREATE', 'announcements', 'Ops auto announcement ' + payload.announcement_id, String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, message: 'Auto-announcement berhasil dibuat.', data: ins.data });
    }

    if (path === 'admin/operations-intelligence/checkin-alert' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const today = ymd();
      const rEmp = await db('GET', 'employees', { select: 'employee_id,nama,email,divisi,jabatan,is_active', order: 'employee_id.asc', limit: 5000 });
      const rToday = await db('GET', 'attendance', { select: 'employee_id', tanggal: 'eq.' + today, limit: 10000 });
      const rLeavesApprovedToday = await db('GET', 'leave_requests', {
        select: 'employee_id',
        status: 'eq.approved',
        and: '(tanggal_mulai.lte.' + today + ',tanggal_selesai.gte.' + today + ')',
        limit: 5000
      });
      if (!rEmp.ok || !rToday.ok || !rLeavesApprovedToday.ok) {
        const err = !rEmp.ok ? rEmp.error : (!rToday.ok ? rToday.error : rLeavesApprovedToday.error);
        return json(res, 500, { ok: false, message: 'Gagal menyiapkan daftar alert check-in.', error: err });
      }
      const idsReq = Array.isArray(b.employee_ids) ? b.employee_ids.map(function(x) { return String(x || '').trim(); }).filter(Boolean) : [];
      const idsSetReq = new Set(idsReq);
      const todaySet = new Set((rToday.data || []).map(function(x) { return String(x.employee_id || ''); }).filter(Boolean));
      const approvedLeaveSet = new Set((rLeavesApprovedToday.data || []).map(function(x) { return String(x.employee_id || ''); }).filter(Boolean));
      const baseTargets = (rEmp.data || []).filter(function(e) {
        const id = String(e.employee_id || '');
        if (!id) return false;
        if (String(e.is_active).toLowerCase() !== 'true') return false;
        if (todaySet.has(id)) return false;
        if (approvedLeaveSet.has(id)) return false;
        return true;
      });
      const targets = idsSetReq.size ? baseTargets.filter(function(e) { return idsSetReq.has(String(e.employee_id || '')); }) : baseTargets;
      let sent = 0;
      const failed = [];
      const app = appBaseUrl();
      for (const e of targets) {
        const email = String(e.email || '').trim().toLowerCase();
        if (!isValidEmail(email)) { failed.push({ employee_id: e.employee_id, reason: 'email_tidak_valid' }); continue; }
        const html = leaveMailTemplate('Reminder Check-in ESS', [
          'Halo ' + String(e.nama || e.employee_id) + ',',
          'Sampai saat ini sistem belum mencatat check-in Anda untuk tanggal <b>' + today + '</b>.',
          'Silakan login ESS dan lakukan check-in sesuai jadwal kerja.',
          'Akses ESS: <a href="' + app + '/employee">' + app + '/employee</a>'
        ]);
        const rs = await sendEssEmail(email, 'Reminder Check-in ESS - ' + today, html);
        if (rs && rs.sent) sent += 1;
        else failed.push({ employee_id: e.employee_id, reason: 'gagal_kirim_email' });
      }
      await auditLog(a.email, 'CREATE', 'ops_checkin_alert', 'Send checkin alert total=' + String(targets.length) + ', sent=' + String(sent), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, {
        ok: true,
        message: 'Pengiriman alert check-in selesai.',
        date: today,
        target_count: targets.length,
        sent_count: sent,
        failed_count: failed.length,
        failed: failed
      });
    }

    if (path === 'admin/operations-intelligence/escalation-digest' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const today = ymd();
      const [summary, rules] = await Promise.all([
        (async function() {
          const rEmp = await db('GET', 'employees', { select: 'employee_id,is_active', limit: 5000 });
          const rToday = await db('GET', 'attendance', { select: 'employee_id', tanggal: 'eq.' + today, limit: 10000 });
          const rLeave = await db('GET', 'leave_requests', { select: 'leave_id', status: 'eq.pending', limit: 5000 });
          if (!rEmp.ok || !rToday.ok || !rLeave.ok) return null;
          const activeCount = (rEmp.data || []).filter(function(x){ return String(x.is_active).toLowerCase()==='true'; }).length;
          const checkedIn = new Set((rToday.data || []).map(function(x){ return String(x.employee_id||''); }).filter(Boolean)).size;
          const pending = Array.isArray(rLeave.data) ? rLeave.data.length : 0;
          return { active_employees: activeCount, checked_in_today: checkedIn, not_checked_in_today: Math.max(0, activeCount-checkedIn), pending_leaves: pending };
        })(),
        db('GET', 'config', { select: 'key,value', key: 'in.(OPS_CHECKIN_GAP_CRITICAL,OPS_PENDING_LEAVES_CRITICAL)', limit: 10 })
      ]);
      if (!summary || !rules.ok) return json(res, 500, { ok: false, message: 'Gagal membuat escalation digest.' });
      const map = {};
      (rules.data || []).forEach(function(x){ map[String(x.key||'')] = Number(x.value||0); });
      const thCheckinCritical = Number(map.OPS_CHECKIN_GAP_CRITICAL || 25);
      const thPendingCritical = Number(map.OPS_PENDING_LEAVES_CRITICAL || 15);
      const noCheckinRate = summary.active_employees > 0 ? Math.round((summary.not_checked_in_today / summary.active_employees) * 10000) / 100 : 0;
      const shouldEscalate = noCheckinRate >= thCheckinCritical || summary.pending_leaves >= thPendingCritical;
      const title = shouldEscalate ? 'Escalation Digest Operasional Harian' : 'Daily Digest Operasional Stabil';
      const detail = 'Tanggal ' + today + ' • Active=' + summary.active_employees + ' • NoCheckin=' + summary.not_checked_in_today + ' (' + noCheckinRate + '%) • PendingLeave=' + summary.pending_leaves + '.';
      return json(res, 200, { ok: true, should_escalate: shouldEscalate, title: title, detail: detail, summary: summary, thresholds: { checkin_critical: thCheckinCritical, pending_critical: thPendingCritical } });
    }

    if (path === 'admin/operations-intelligence/escalation-digest/publish' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const title = String(b.title || '').trim();
      const detail = String(b.detail || '').trim();
      if (!title || !detail) return json(res, 400, { ok: false, message: 'title dan detail escalation wajib diisi.' });
      const payload = {
        announcement_id: rid('ANN'),
        judul: '[OPS DIGEST] ' + title,
        isi: detail + '\n\nSumber: Escalation Digest',
        target_role: 'admin',
        published_at: nowIso(),
        expired_at: null,
        is_active: true,
        created_by: a.email
      };
      const ins = await db('POST', 'announcements', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal publish escalation digest.', error: ins.error });
      return json(res, 200, { ok: true, message: 'Escalation digest dipublish.', data: ins.data });
    }

    if (path === 'admin/operations-intelligence/summary' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const today = ymd();
      const startDate = String(req.query.start_date || dateShift(today, -13)).trim();
      const endDate = String(req.query.end_date || today).trim();
      const rCfg = await db('GET', 'config', { select: 'key,value', key: 'in.(LATE_AFTER_TIME,OPS_CHECKIN_GAP_HIGH,OPS_CHECKIN_GAP_CRITICAL,OPS_PENDING_LEAVES_MEDIUM,OPS_PENDING_LEAVES_CRITICAL,OPS_LATE_RATE_HIGH,OPS_DIVISION_RISK_MEDIUM)', limit: 100 });
      const rEmp = await db('GET', 'employees', { select: 'employee_id,nama,email,divisi,jabatan,is_active', order: 'employee_id.asc', limit: 5000 });
      const rToday = await db('GET', 'attendance', { select: 'employee_id,jam_masuk,jam_keluar,status,tanggal', tanggal: 'eq.' + today, order: 'created_at.desc', limit: 10000 });
      const rLeavesPending = await db('GET', 'leave_requests', { select: 'leave_id,employee_id,jenis_cuti,tanggal_mulai,tanggal_selesai,status,created_at', status: 'eq.pending', order: 'created_at.desc', limit: 5000 });
      const rLeavesApprovedToday = await db('GET', 'leave_requests', {
        select: 'employee_id',
        status: 'eq.approved',
        and: '(tanggal_mulai.lte.' + today + ',tanggal_selesai.gte.' + today + ')',
        limit: 5000
      });
      const rAttendancePeriod = await db('GET', 'attendance', {
        select: 'employee_id,tanggal,jam_masuk,status',
        order: 'tanggal.asc,created_at.asc',
        limit: 30000,
        and: '(tanggal.gte.' + startDate + ',tanggal.lte.' + endDate + ')'
      });
      if (!rCfg.ok || !rEmp.ok || !rToday.ok || !rLeavesPending.ok || !rLeavesApprovedToday.ok || !rAttendancePeriod.ok) {
        const err = !rCfg.ok ? rCfg.error : !rEmp.ok ? rEmp.error : !rToday.ok ? rToday.error : !rLeavesPending.ok ? rLeavesPending.error : (!rLeavesApprovedToday.ok ? rLeavesApprovedToday.error : rAttendancePeriod.error);
        return json(res, 500, { ok: false, message: 'Gagal menghitung operations intelligence.', error: err });
      }
      const cfgMap = {};
      (rCfg.data || []).forEach(function(x) { cfgMap[String(x.key || '')] = String(x.value || ''); });
      const lateAfter = cfgMap.LATE_AFTER_TIME || '08:30:00';
      const thCheckinHigh = Number(cfgMap.OPS_CHECKIN_GAP_HIGH || 10);
      const thCheckinCritical = Number(cfgMap.OPS_CHECKIN_GAP_CRITICAL || 25);
      const thPendingMedium = Number(cfgMap.OPS_PENDING_LEAVES_MEDIUM || 5);
      const thPendingCritical = Number(cfgMap.OPS_PENDING_LEAVES_CRITICAL || 15);
      const thLateHigh = Number(cfgMap.OPS_LATE_RATE_HIGH || 20);
      const thDivMedium = Number(cfgMap.OPS_DIVISION_RISK_MEDIUM || 20);
      const emps = rEmp.data || [];
      const todayRows = rToday.data || [];
      const leavesPending = rLeavesPending.data || [];
      const periodRows = rAttendancePeriod.data || [];
      const activeEmployees = emps.filter(function(e) { return String(e.is_active).toLowerCase() === 'true'; });
      const empMap = {};
      emps.forEach(function(e) { empMap[String(e.employee_id || '')] = e; });
      const todayMap = {};
      todayRows.forEach(function(r) { if (!todayMap[r.employee_id]) todayMap[r.employee_id] = r; });
      const approvedLeaveSet = new Set((rLeavesApprovedToday.data || []).map(function(x) { return String(x.employee_id || ''); }).filter(Boolean));
      const noCheckInEmployees = activeEmployees.filter(function(e) {
        const id = String(e.employee_id || '');
        if (!id) return false;
        if (todayMap[id]) return false;
        if (approvedLeaveSet.has(id)) return false;
        return true;
      }).map(function(e) {
        return {
          employee_id: String(e.employee_id || ''),
          nama: String(e.nama || ''),
          email: String(e.email || ''),
          divisi: String(e.divisi || ''),
          jabatan: String(e.jabatan || '')
        };
      });
      const checkedInCount = activeEmployees.length - noCheckInEmployees.length;
      const noCheckInCount = noCheckInEmployees.length;
      const noCheckInRate = activeEmployees.length > 0 ? Math.round((noCheckInCount / activeEmployees.length) * 10000) / 100 : 0;
      let lateCountPeriod = 0;
      const divLate = {};
      periodRows.forEach(function(r) {
        const st = String(r.status || '').toLowerCase();
        const jm = String(r.jam_masuk || '');
        const isLate = st === 'terlambat' || (jm && jm > lateAfter);
        const e = empMap[String(r.employee_id || '')] || {};
        const div = String(e.divisi || 'Tanpa Divisi');
        if (!divLate[div]) divLate[div] = { divisi: div, total: 0, late: 0, late_rate: 0 };
        divLate[div].total += 1;
        if (isLate) { divLate[div].late += 1; lateCountPeriod += 1; }
      });
      Object.keys(divLate).forEach(function(k) {
        const d = divLate[k];
        d.late_rate = d.total > 0 ? Math.round((d.late / d.total) * 10000) / 100 : 0;
      });
      const periodLateRate = periodRows.length > 0 ? Math.round((lateCountPeriod / periodRows.length) * 10000) / 100 : 0;
      const topRiskDivisions = Object.values(divLate).sort(function(a1, b1) { return Number(b1.late_rate || 0) - Number(a1.late_rate || 0); }).slice(0, 5);
      const alerts = [];
      if (noCheckInRate >= thCheckinCritical) alerts.push({ severity: 'critical', code: 'CHECKIN_GAP', title: 'Kesenjangan Check-in Tinggi', detail: noCheckInCount + ' karyawan aktif belum check-in hari ini (' + noCheckInRate + '%).', action_route: 'att', action_label: 'Buka Attendance Log' });
      else if (noCheckInRate >= thCheckinHigh) alerts.push({ severity: 'high', code: 'CHECKIN_GAP', title: 'Check-in Belum Optimal', detail: noCheckInCount + ' karyawan aktif belum check-in hari ini (' + noCheckInRate + '%).', action_route: 'att', action_label: 'Lihat Status Hari Ini' });
      if (leavesPending.length >= thPendingCritical) alerts.push({ severity: 'critical', code: 'PENDING_LEAVES', title: 'Antrian Approval Cuti Tinggi', detail: leavesPending.length + ' pengajuan cuti menunggu approval.', action_route: 'leave', action_label: 'Proses Leave Approvals' });
      else if (leavesPending.length >= thPendingMedium) alerts.push({ severity: 'medium', code: 'PENDING_LEAVES', title: 'Approval Cuti Perlu Ditinjau', detail: leavesPending.length + ' pengajuan cuti masih pending.', action_route: 'leave', action_label: 'Review Pengajuan Cuti' });
      if (periodLateRate >= thLateHigh) alerts.push({ severity: 'high', code: 'LATE_RATE', title: 'Keterlambatan Periode Tinggi', detail: 'Late rate periode ' + startDate + ' s/d ' + endDate + ' mencapai ' + periodLateRate + '%.', action_route: 'kpi', action_label: 'Analisis KPI HR' });
      const riskDivision = topRiskDivisions[0] || null;
      if (riskDivision && Number(riskDivision.late_rate || 0) >= thDivMedium) alerts.push({ severity: 'medium', code: 'DIVISION_RISK', title: 'Divisi Risiko Tinggi', detail: riskDivision.divisi + ' memiliki late rate ' + riskDivision.late_rate + '%.', action_route: 'kpi', action_label: 'Buka Heatmap Divisi' });
      if (!alerts.length) alerts.push({ severity: 'info', code: 'STABLE', title: 'Operasional Stabil', detail: 'Tidak ada indikator kritikal pada periode ini.', action_route: 'home', action_label: 'Tetap Pantau Dashboard' });
      const recommendations = [];
      if (noCheckInRate >= thCheckinHigh) recommendations.push({ priority: 1, text: 'Prioritaskan follow-up check-in ke unit dengan kehadiran rendah sebelum jam operasional berakhir.' });
      if (leavesPending.length >= thPendingMedium) recommendations.push({ priority: 2, text: 'Jadwalkan batch approval cuti untuk menekan backlog dan mengurangi bottleneck HR.' });
      if (periodLateRate >= (thLateHigh - 5)) recommendations.push({ priority: 3, text: 'Lakukan evaluasi aturan keterlambatan per divisi dan aktifkan coaching untuk tim berisiko.' });
      if (!recommendations.length) recommendations.push({ priority: 1, text: 'Pertahankan disiplin kehadiran; lakukan review KPI mingguan untuk menjaga tren positif.' });
      recommendations.sort(function(a1, b1) { return Number(a1.priority || 99) - Number(b1.priority || 99); });
      return json(res, 200, {
        ok: true,
        generated_at: nowIso(),
        period: { start_date: startDate, end_date: endDate },
        summary: {
          active_employees: activeEmployees.length,
          checked_in_today: checkedInCount,
          not_checked_in_today: noCheckInCount,
          no_check_in_rate: noCheckInRate,
          pending_leaves: leavesPending.length,
          attendance_records_period: periodRows.length,
          late_records_period: lateCountPeriod,
          late_rate_period: periodLateRate,
          late_after_time: lateAfter
        },
        not_checked_in_employees: noCheckInEmployees,
        rules: {
          checkin_gap_high: thCheckinHigh,
          checkin_gap_critical: thCheckinCritical,
          pending_leaves_medium: thPendingMedium,
          pending_leaves_critical: thPendingCritical,
          late_rate_high: thLateHigh,
          division_risk_medium: thDivMedium
        },
        top_risk_divisions: topRiskDivisions,
        alerts: alerts,
        recommendations: recommendations
      });
    }

    if (path === 'admin/reports/employees' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const q = { select: '*', order: 'employee_id.asc', limit: Math.min(Number(req.query.limit || 2000), 5000) };
      if (req.query.role) q.role = 'eq.' + String(req.query.role || '').trim().toLowerCase();
      if (req.query.divisi) q.divisi = 'eq.' + String(req.query.divisi || '').trim();
      if (req.query.is_active !== undefined && req.query.is_active !== '') q.is_active = 'eq.' + (String(req.query.is_active).toLowerCase() === 'true' ? 'true' : 'false');
      const startDate = String(req.query.start_date || '').trim();
      const endDate = String(req.query.end_date || '').trim();
      if (startDate && endDate) q.and = '(tanggal_masuk.gte.' + startDate + ',tanggal_masuk.lte.' + endDate + ')';
      else if (startDate) q.tanggal_masuk = 'gte.' + startDate;
      else if (endDate) q.tanggal_masuk = 'lte.' + endDate;
      const r = await db('GET', 'employees', q);
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil report employees.', error: r.error });
      const rows = r.data || [];
      const summary = {
        total: rows.length,
        active: rows.filter(function(x) { return String(x.is_active).toLowerCase() === 'true'; }).length,
        inactive: rows.filter(function(x) { return String(x.is_active).toLowerCase() !== 'true'; }).length,
        employee: rows.filter(function(x) { return String(x.role || '').toLowerCase() === 'employee'; }).length,
        admin: rows.filter(function(x) { return String(x.role || '').toLowerCase() === 'admin'; }).length,
        superadmin: rows.filter(function(x) { return String(x.role || '').toLowerCase() === 'superadmin'; }).length
      };
      await auditLog(a.email, 'REPORT', 'employees', 'Generate report employees total=' + String(summary.total), String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || ''));
      return json(res, 200, { ok: true, summary: summary, rows: rows });
    }

    return json(res, 404, { ok: false, message: 'Endpoint tidak ditemukan.', path: path, method: method });
  } catch (err) {
    return json(res, 500, { ok: false, message: err.message || 'Terjadi kesalahan.' });
  }
};
