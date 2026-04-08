const { json, supabaseInsert, toIsoNow, randomId } = require('../_supabase');

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    return json(res, 405, { ok: false, message: 'Method tidak didukung.' });
  }

  let body = req.body;
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (err) {
      return json(res, 400, { ok: false, message: 'Body JSON tidak valid.' });
    }
  }

  const email = String((body && body.email) || '').trim().toLowerCase();
  const employeeId = String((body && body.employee_id) || '').trim();
  const lokasi = String((body && body.lokasi) || '').trim();
  const status = String((body && body.status) || 'Hadir').trim();
  const photoUrl = String((body && body.foto_masuk_url) || '').trim();
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  const tanggal = String((body && body.tanggal) || (yyyy + '-' + mm + '-' + dd)).trim();
  const jamMasuk = String((body && body.jam_masuk) || now.toTimeString().slice(0, 8)).trim();

  if (!email || !employeeId) {
    return json(res, 400, { ok: false, message: 'employee_id dan email wajib diisi.' });
  }

  const payload = {
    attendance_id: randomId('ATD'),
    employee_id: employeeId,
    email: email,
    tanggal: tanggal,
    jam_masuk: jamMasuk,
    status: status,
    lokasi: lokasi,
    foto_masuk_url: photoUrl,
    created_at: toIsoNow(),
    updated_at: toIsoNow()
  };

  const inserted = await supabaseInsert('attendance', payload);
  if (!inserted.ok) {
    return json(res, 500, {
      ok: false,
      message: 'Gagal menyimpan check-in ke Supabase.',
      error: inserted.error
    });
  }

  return json(res, 200, {
    ok: true,
    message: 'Check-in berhasil disimpan ke Supabase.',
    data: inserted.data
  });
};
