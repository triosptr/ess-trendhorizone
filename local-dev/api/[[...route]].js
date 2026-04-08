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
function ymd() {
  const d = new Date();
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}
function hms() {
  const d = new Date();
  return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0') + ':' + String(d.getSeconds()).padStart(2, '0');
}
function rid(prefix) { return prefix + '_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }
function roleAdmin(role) { const v = String(role || '').toLowerCase(); return v === 'superadmin' || v === 'admin'; }

function userCtx(req) {
  const q = req.query || {};
  return {
    email: String(req.headers['x-user-email'] || q.email || '').trim().toLowerCase(),
    employee_id: String(req.headers['x-employee-id'] || q.employee_id || '').trim(),
    role: String(req.headers['x-user-role'] || q.role || 'employee').trim().toLowerCase()
  };
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
  const existing = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + user.employee_id, tanggal: 'eq.' + tanggal, limit: 1, order: 'created_at.desc' });
  if (!existing.ok) return json(res, 500, { ok: false, message: 'Gagal cek attendance.', error: existing.error });
  const row = Array.isArray(existing.data) && existing.data[0] ? existing.data[0] : null;
  if (row && row.jam_masuk) return json(res, 400, { ok: false, message: 'Anda sudah check-in hari ini.' });

  const payload = {
    attendance_id: rid('ATD'),
    employee_id: user.employee_id,
    email: user.email,
    tanggal: tanggal,
    jam_masuk: String(body.jam_masuk || hms()).trim(),
    status: String(body.status || 'Hadir').trim(),
    lokasi: String(body.lokasi || '').trim(),
    work_mode: String(body.work_mode || '').trim().toLowerCase(),
    foto_masuk_url: String(body.foto_masuk_url || '').trim(),
    catatan: String(body.catatan || '').trim(),
    created_at: nowIso(),
    updated_at: nowIso()
  };
  const ins = await db('POST', 'attendance', null, payload, { Prefer: 'return=representation' });
  if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal check-in.', error: ins.error });
  return json(res, 200, { ok: true, message: 'Check-in berhasil.', data: ins.data });
}

async function handleMeAttendanceCheckOut(req, res, user) {
  const body = await readBody(req);
  const tanggal = String(body.tanggal || ymd()).trim();
  const existing = await db('GET', 'attendance', { select: '*', employee_id: 'eq.' + user.employee_id, tanggal: 'eq.' + tanggal, limit: 1, order: 'created_at.desc' });
  if (!existing.ok) return json(res, 500, { ok: false, message: 'Gagal cek attendance.', error: existing.error });
  const row = Array.isArray(existing.data) && existing.data[0] ? existing.data[0] : null;
  if (!row || !row.attendance_id || !row.jam_masuk) return json(res, 400, { ok: false, message: 'Belum check-in hari ini.' });
  if (row.jam_keluar) return json(res, 400, { ok: false, message: 'Sudah check-out hari ini.' });

  const patch = await db('PATCH', 'attendance', { attendance_id: 'eq.' + row.attendance_id }, {
    jam_keluar: String(body.jam_keluar || hms()).trim(),
    status: String(body.status || row.status || 'Hadir').trim(),
    lokasi: String(body.lokasi || row.lokasi || '').trim(),
    foto_keluar_url: String(body.foto_keluar_url || '').trim(),
    catatan: String(body.catatan || row.catatan || '').trim(),
    updated_at: nowIso()
  }, { Prefer: 'return=representation' });
  if (!patch.ok) return json(res, 500, { ok: false, message: 'Gagal check-out.', error: patch.error });
  return json(res, 200, { ok: true, message: 'Check-out berhasil.', data: patch.data });
}

module.exports = async function handler(req, res) {
  try {
    const path = routePath(req);
    const method = String(req.method || 'GET').toUpperCase();

    if (path === 'health' && method === 'GET') {
      return json(res, 200, { ok: true, service: 'ess-trendhorizone-api', supabase_ready: !!env(), timestamp: nowIso() });
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
      const row = (r.data && r.data[0]) || null;
      if (!row) return json(res, 404, { ok: false, message: 'Profile tidak ditemukan.' });
      return json(res, 200, row);
    }

    if (path === 'me/dashboard-summary' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const p = await db('GET', 'employees', { select: 'employee_id,nama,divisi,jabatan,jatah_cuti,sisa_cuti', employee_id: 'eq.' + u.employee_id, limit: 1 });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal ambil summary.', error: p.error });
      const row = (p.data && p.data[0]) || null;
      if (!row) return json(res, 404, { ok: false, message: 'Karyawan tidak ditemukan.' });
      const leaves = await db('GET', 'leave_requests', { select: 'leave_id', employee_id: 'eq.' + u.employee_id, status: 'eq.pending' });
      if (!leaves.ok) return json(res, 500, { ok: false, message: 'Gagal ambil pending leaves.', error: leaves.error });
      return json(res, 200, Object.assign({}, row, { pendingLeaves: Array.isArray(leaves.data) ? leaves.data.length : 0 }));
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
      if (!row) return json(res, 200, { attendance_id: '', employee_id: u.employee_id, email: u.email, tanggal: tanggal, jam_masuk: '', jam_keluar: '', status: 'Belum Absen', lokasi: '', work_mode: '', foto_masuk_url: '', foto_keluar_url: '', catatan: '' });
      return json(res, 200, row);
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
      const payload = {
        leave_id: rid('LEAVE'),
        employee_id: u.employee_id,
        email: u.email,
        jenis_cuti: String(b.jenis_cuti || '').trim(),
        tanggal_mulai: String(b.tanggal_mulai || '').trim(),
        tanggal_selesai: String(b.tanggal_selesai || '').trim(),
        jumlah_hari: Number(b.jumlah_hari || 0),
        alasan: String(b.alasan || '').trim(),
        lampiran_url: String(b.lampiran_url || '').trim(),
        status: 'pending',
        approver_email: String(b.approver_email || '').trim(),
        created_at: nowIso(),
        updated_at: nowIso()
      };
      if (!payload.jenis_cuti || !payload.tanggal_mulai || !payload.tanggal_selesai) return json(res, 400, { ok: false, message: 'jenis_cuti, tanggal_mulai, tanggal_selesai wajib diisi.' });
      const ins = await db('POST', 'leave_requests', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal submit leave.', error: ins.error });
      return json(res, 200, { ok: true, message: 'Pengajuan cuti berhasil dikirim.', data: ins.data });
    }

    if (path === 'me/payroll-docs' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const r = await db('GET', 'payroll_docs', { select: '*', employee_id: 'eq.' + u.employee_id, order: 'uploaded_at.desc', limit: Math.min(Number(req.query.limit || 24), 100) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil payroll docs.', error: r.error });
      return json(res, 200, r.data || []);
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
        const up = await db('POST', 'notification_seen', null, [{ email: u.email }], { Prefer: 'resolution=merge-duplicates,return=representation', 'On-Conflict': 'email' });
        if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal init notification seen.', error: up.error });
        row = (up.data && up.data[0]) || { email: u.email };
      }
      const ann = await db('GET', 'announcements', { select: 'announcement_id,published_at', is_active: 'eq.true', or: '(target_role.eq.all,target_role.eq.' + u.role + ')' });
      const pay = await db('GET', 'payroll_docs', { select: 'doc_id,uploaded_at', employee_id: 'eq.' + u.employee_id });
      if (!ann.ok || !pay.ok) return json(res, 500, { ok: false, message: 'Gagal hitung notifikasi.', error: ann.ok ? pay.error : ann.error });
      const annTs = new Date(row.announcement_seen_at || 0).getTime() || 0;
      const payTs = new Date(row.payroll_seen_at || 0).getTime() || 0;
      const unreadA = (ann.data || []).filter(function(x) { return (new Date(x.published_at || 0).getTime() || 0) > annTs; }).length;
      const unreadP = (pay.data || []).filter(function(x) { return (new Date(x.uploaded_at || 0).getTime() || 0) > payTs; }).length;
      return json(res, 200, { unread_announcements: unreadA, unread_payroll_docs: unreadP, total_unread: unreadA + unreadP });
    }

    if (path === 'me/notifications/mark-seen' && method === 'POST') {
      const u = requireUser(req, res); if (!u) return;
      const b = await readBody(req);
      const type = String(b.type || 'all').toLowerCase();
      const payload = { email: u.email, updated_at: nowIso() };
      if (type === 'all' || type === 'announcement') payload.announcement_seen_at = nowIso();
      if (type === 'all' || type === 'payroll') payload.payroll_seen_at = nowIso();
      const up = await db('POST', 'notification_seen', null, [payload], { Prefer: 'resolution=merge-duplicates,return=representation', 'On-Conflict': 'email' });
      if (!up.ok) return json(res, 500, { ok: false, message: 'Gagal mark seen.', error: up.error });
      return json(res, 200, { ok: true, message: 'Notifikasi ditandai dilihat.' });
    }

    if (path === 'me/notifications' && method === 'GET') {
      const u = requireUser(req, res); if (!u) return;
      const seen = await db('GET', 'notification_seen', { select: '*', email: 'eq.' + u.email, limit: 1 });
      const row = seen.ok && seen.data && seen.data[0] ? seen.data[0] : { announcement_seen_at: null, payroll_seen_at: null };
      const ann = await db('GET', 'announcements', { select: 'announcement_id,judul,isi,published_at,target_role,is_active', is_active: 'eq.true', or: '(target_role.eq.all,target_role.eq.' + u.role + ')' });
      const pay = await db('GET', 'payroll_docs', { select: 'doc_id,nama_file,bulan,tahun,file_url,uploaded_at', employee_id: 'eq.' + u.employee_id });
      if (!ann.ok || !pay.ok) return json(res, 500, { ok: false, message: 'Gagal ambil notifikasi.', error: ann.ok ? pay.error : ann.error });
      const annTs = new Date(row.announcement_seen_at || 0).getTime() || 0;
      const payTs = new Date(row.payroll_seen_at || 0).getTime() || 0;
      const notifications = [];
      (ann.data || []).forEach(function(x) {
        const ts = new Date(x.published_at || 0).getTime() || 0;
        notifications.push({ notification_type: 'announcement', item_id: x.announcement_id || '', title: x.judul || 'Pengumuman Baru', message: x.isi || '', date_value: x.published_at || '', is_unread: ts > annTs ? 'TRUE' : 'FALSE', action_label: 'Lihat Pengumuman' });
      });
      (pay.data || []).forEach(function(x) {
        const ts = new Date(x.uploaded_at || 0).getTime() || 0;
        notifications.push({ notification_type: 'payroll', item_id: x.doc_id || '', title: 'Slip Gaji Baru Tersedia', message: (x.nama_file || 'Dokumen payroll') + ' • ' + (x.bulan || '-') + ' ' + (x.tahun || '-'), date_value: x.uploaded_at || '', is_unread: ts > payTs ? 'TRUE' : 'FALSE', file_url: x.file_url || '', action_label: 'Buka Slip Gaji' });
      });
      notifications.sort(function(a, b) { return new Date(b.date_value || 0).getTime() - new Date(a.date_value || 0).getTime(); });
      return json(res, 200, notifications);
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
      const payload = {
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
        is_active: b.is_active === undefined ? true : String(b.is_active).toLowerCase() === 'true',
        no_hp: String(b.no_hp || '').trim(),
        alamat: String(b.alamat || '').trim(),
        created_at: nowIso(),
        updated_at: nowIso()
      };
      if (!payload.email || !payload.nama) return json(res, 400, { ok: false, message: 'email dan nama wajib diisi.' });
      const ins = await db('POST', 'employees', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal tambah employee.', error: ins.error });
      return json(res, 200, { ok: true, message: 'Employee berhasil ditambahkan.', data: ins.data });
    }

    if (path === 'admin/attendance/today' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await db('GET', 'attendance', { select: '*', tanggal: 'eq.' + String(req.query.tanggal || ymd()), order: 'created_at.desc', limit: Math.min(Number(req.query.limit || 500), 2000) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil attendance today.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'admin/leaves/pending' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await db('GET', 'leave_requests', { select: '*', status: 'eq.pending', order: 'created_at.asc', limit: Math.min(Number(req.query.limit || 300), 1000) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil cuti pending.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'admin/leaves' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const q = { select: '*', order: 'created_at.desc', limit: Math.min(Number(req.query.limit || 500), 2000) };
      if (req.query.status) q.status = 'eq.' + String(req.query.status).toLowerCase();
      const r = await db('GET', 'leave_requests', q);
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil leaves.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'admin/leaves/approve' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const leaveId = String(b.leave_id || '').trim();
      if (!leaveId) return json(res, 400, { ok: false, message: 'leave_id wajib diisi.' });
      const p = await db('PATCH', 'leave_requests', { leave_id: 'eq.' + leaveId }, { status: 'approved', approver_email: a.email, approved_at: nowIso(), catatan_approver: String(b.catatan_approver || '').trim(), updated_at: nowIso() }, { Prefer: 'return=representation' });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal approve.', error: p.error });
      return json(res, 200, { ok: true, message: 'Pengajuan cuti berhasil disetujui.', data: p.data });
    }

    if (path === 'admin/leaves/reject' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const leaveId = String(b.leave_id || '').trim();
      if (!leaveId) return json(res, 400, { ok: false, message: 'leave_id wajib diisi.' });
      const p = await db('PATCH', 'leave_requests', { leave_id: 'eq.' + leaveId }, { status: 'rejected', approver_email: a.email, approved_at: nowIso(), catatan_approver: String(b.catatan_approver || '').trim(), updated_at: nowIso() }, { Prefer: 'return=representation' });
      if (!p.ok) return json(res, 500, { ok: false, message: 'Gagal reject.', error: p.error });
      return json(res, 200, { ok: true, message: 'Pengajuan cuti berhasil ditolak.', data: p.data });
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
      const ins = await db('POST', 'announcements', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal membuat announcement.', error: ins.error });
      return json(res, 200, { ok: true, message: 'Announcement berhasil dibuat.', data: ins.data });
    }

    if (path === 'admin/payroll-docs' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await db('GET', 'payroll_docs', { select: '*', order: 'uploaded_at.desc', limit: Math.min(Number(req.query.limit || 300), 1000) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil payroll docs.', error: r.error });
      return json(res, 200, r.data || []);
    }

    if (path === 'admin/payroll-docs' && method === 'POST') {
      const a = requireAdmin(req, res); if (!a) return;
      const b = await readBody(req);
      const payload = { doc_id: rid('PAY'), employee_id: String(b.employee_id || '').trim(), email: String(b.email || '').trim().toLowerCase(), bulan: String(b.bulan || '').trim(), tahun: String(b.tahun || '').trim(), nama_file: String(b.nama_file || '').trim(), file_url: String(b.file_url || '').trim(), keterangan: String(b.keterangan || '').trim(), uploaded_at: b.uploaded_at || nowIso() };
      if (!payload.employee_id || !payload.file_url || !payload.nama_file) return json(res, 400, { ok: false, message: 'employee_id, nama_file, file_url wajib diisi.' });
      const ins = await db('POST', 'payroll_docs', null, payload, { Prefer: 'return=representation' });
      if (!ins.ok) return json(res, 500, { ok: false, message: 'Gagal membuat payroll doc.', error: ins.error });
      return json(res, 200, { ok: true, message: 'Payroll doc berhasil dibuat.', data: ins.data });
    }

    if (path === 'admin/master/divisions' || path === 'admin/master/positions' || path === 'admin/master/leave-types') {
      const a = requireAdmin(req, res); if (!a) return;
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

    if (path === 'admin/reports/employees' && method === 'GET') {
      const a = requireAdmin(req, res); if (!a) return;
      const r = await db('GET', 'employees', { select: '*', order: 'employee_id.asc', limit: Math.min(Number(req.query.limit || 2000), 5000) });
      if (!r.ok) return json(res, 500, { ok: false, message: 'Gagal ambil report employees.', error: r.error });
      return json(res, 200, { ok: true, rows: r.data || [] });
    }

    return json(res, 404, { ok: false, message: 'Endpoint tidak ditemukan.', path: path, method: method });
  } catch (err) {
    return json(res, 500, { ok: false, message: err.message || 'Terjadi kesalahan.' });
  }
};
