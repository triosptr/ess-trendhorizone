const ESS_CONFIG = {
  APP_NAME: 'ESS Karyawan 2026',
  TEST_MODE: true,
  TEST_EMAIL: 'programmer1@perusahaan.com',
  WEB_DASHBOARD_BASE_URL: 'https://ess-2026-trendhorizone-id.vercel.app',

  LEAVE_ATTACHMENT_FOLDER_ID: '1mpbIt5CEkPOVFGd1MXHSh1VnDs2hwoKy',
  MAX_ATTACHMENT_SIZE_MB: 10,

  // =========================
  // ABSENSI CONFIG
  // =========================
  ATTENDANCE_PHOTO_FOLDER_ID: '1cI6C6okEJDfQbfoIodJg2kUlDmMScRng',

  // Jam masuk normal
  WORK_START_TIME: '08:00:00',

  // Batas telat
  LATE_AFTER_TIME: '08:30:00'
};

function doGet(e) {
  const user = getCurrentUser_();

  if (!user) {
    return HtmlService
      .createTemplateFromFile('AccessDenied')
      .evaluate()
      .setTitle('Akses Ditolak');
  }

  const templateName = String(user.role).trim().toLowerCase() === 'superadmin'
    ? 'AdminDashboard'
    : 'EmployeeDashboard';

  const newDashboardUrl = buildNewDashboardUrl_(user);
  const requestedTheme = String((e && e.parameter && e.parameter.theme) || '').trim().toLowerCase();
  if (requestedTheme === 'v2' && newDashboardUrl) {
    return HtmlService.createHtmlOutput(
      '<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Redirecting...</title></head><body><script>window.location.replace('
      + JSON.stringify(newDashboardUrl)
      + ');</script></body></html>'
    ).setTitle(ESS_CONFIG.APP_NAME);
  }

  const template = HtmlService.createTemplateFromFile(templateName);
  template.appName = ESS_CONFIG.APP_NAME;
  template.user = user;
  template.newDashboardUrl = newDashboardUrl;

  return template
    .evaluate()
    .setTitle(ESS_CONFIG.APP_NAME)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.DEFAULT);
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

function buildNewDashboardUrl_(user) {
  const base = String(ESS_CONFIG.WEB_DASHBOARD_BASE_URL || '').trim().replace(/\/+$/, '');
  if (!base || !user) return '';
  const role = String(user.role || '').trim().toLowerCase();
  const params = [];
  if (user.email) params.push('email=' + encodeURIComponent(String(user.email)));
  if (role === 'superadmin') {
    params.push('role=superadmin');
    return base + '/admin.preview.html' + (params.length ? ('?' + params.join('&')) : '');
  }
  if (user.employee_id) params.push('employee_id=' + encodeURIComponent(String(user.employee_id)));
  params.push('role=employee');
  return base + '/employee.preview.html' + (params.length ? ('?' + params.join('&')) : '');
}
