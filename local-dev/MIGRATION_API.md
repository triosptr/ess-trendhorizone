# ESS Trendhorizone API (Vercel + Supabase)

Base URL production:

- `https://ess-2026-trendhorizone-id.vercel.app`

Header identitas user untuk endpoint `me/*`:

- `x-user-email`
- `x-employee-id`
- `x-user-role`

Endpoint utama:

- `GET /api/health`
- `GET /api/auth/me`
- `GET /api/me/profile`
- `GET /api/me/dashboard-summary`
- `GET /api/me/attendance/config`
- `GET /api/me/attendance/today`
- `GET /api/me/attendance/history`
- `POST /api/me/attendance/check-in`
- `POST /api/me/attendance/check-out`
- `GET /api/leave-types/active`
- `GET /api/me/leaves`
- `POST /api/me/leaves`
- `GET /api/me/announcements`
- `GET /api/me/payroll-docs`
- `GET /api/me/notifications/summary`
- `GET /api/me/notifications`
- `POST /api/me/notifications/mark-seen`
- `GET /api/admin/employees`
- `POST /api/admin/employees`
- `GET /api/admin/attendance/today`
- `GET /api/admin/leaves/pending`
- `GET /api/admin/leaves`
- `POST /api/admin/leaves/approve`
- `POST /api/admin/leaves/reject`
- `GET /api/admin/announcements`
- `POST /api/admin/announcements`
- `GET /api/admin/payroll-docs`
- `POST /api/admin/payroll-docs`
- `GET /api/admin/master/divisions`
- `POST /api/admin/master/divisions`
- `PATCH /api/admin/master/divisions`
- `GET /api/admin/master/positions`
- `POST /api/admin/master/positions`
- `PATCH /api/admin/master/positions`
- `GET /api/admin/master/leave-types`
- `POST /api/admin/master/leave-types`
- `PATCH /api/admin/master/leave-types`
- `GET /api/admin/notifications/leave/summary`
- `GET /api/admin/notifications/leave`
- `POST /api/admin/notifications/leave/mark-seen`
- `GET /api/admin/reports/employees`

Catatan:

- Jalankan ulang SQL terbaru dari `local-dev/supabase/schema.sql`.
- Jika endpoint `me/*` mengembalikan 401, pastikan header user sudah dikirim.
