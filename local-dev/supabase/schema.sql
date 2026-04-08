create extension if not exists "pgcrypto";

create table if not exists employees (
  employee_id text primary key,
  email text not null unique,
  nama text not null,
  nik text,
  divisi text,
  jabatan text,
  atasan_email text,
  status_karyawan text,
  tanggal_masuk date,
  jatah_cuti integer default 12,
  sisa_cuti integer default 12,
  role text default 'employee',
  is_active boolean default true,
  no_hp text,
  alamat text,
  tempat_lahir text,
  tanggal_lahir date,
  jenis_kelamin text,
  npwp text,
  bpjs text,
  bank text,
  no_rekening text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists attendance (
  attendance_id text primary key,
  employee_id text not null references employees(employee_id) on update cascade on delete restrict,
  email text not null,
  tanggal date not null,
  jam_masuk time,
  jam_keluar time,
  status text,
  lokasi text,
  work_mode text,
  foto_masuk_url text,
  foto_keluar_url text,
  catatan text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table if exists attendance add column if not exists work_mode text;

create table if not exists leave_requests (
  leave_id text primary key,
  employee_id text not null references employees(employee_id) on update cascade on delete restrict,
  email text not null,
  jenis_cuti text not null,
  tanggal_mulai date not null,
  tanggal_selesai date not null,
  jumlah_hari numeric(6,2),
  alasan text,
  lampiran_url text,
  status text default 'pending',
  approver_email text,
  approved_at timestamptz,
  catatan_approver text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists audit_log (
  log_id text primary key,
  timestamp timestamptz default now(),
  user_email text,
  aksi text,
  modul text,
  detail text,
  ip_info text
);

create table if not exists config (
  key text primary key,
  value text,
  description text
);

create table if not exists payroll_docs (
  doc_id text primary key,
  employee_id text not null references employees(employee_id) on update cascade on delete restrict,
  email text not null,
  bulan text,
  tahun text,
  nama_file text,
  file_url text not null,
  keterangan text,
  uploaded_at timestamptz default now()
);

create table if not exists announcements (
  announcement_id text primary key,
  judul text not null,
  isi text not null,
  target_role text default 'all',
  published_at timestamptz default now(),
  expired_at timestamptz,
  is_active boolean default true,
  created_by text
);

create table if not exists divisions (
  division_id text primary key,
  nama_divisi text not null,
  kepala_divisi_email text,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists positions (
  position_id text primary key,
  nama_jabatan text not null,
  division_id text,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists leave_types (
  leave_type_id text primary key,
  nama_jenis_cuti text not null,
  maks_hari numeric(6,2) default 0,
  is_paid boolean default true,
  requires_attachment boolean default false,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists notification_seen (
  email text primary key,
  announcement_seen_at timestamptz default to_timestamp(0),
  payroll_seen_at timestamptz default to_timestamp(0),
  updated_at timestamptz default now()
);

insert into config(key, value, description) values
  ('WORK_START_TIME', '08:00:00', 'Jam kerja mulai'),
  ('LATE_AFTER_TIME', '08:30:00', 'Batas terlambat'),
  ('PHOTO_FOLDER_ENABLED', 'true', 'Validasi upload foto')
on conflict (key) do nothing;

insert into leave_types(leave_type_id, nama_jenis_cuti, maks_hari, is_paid, requires_attachment, is_active)
values
  ('LT_ANNUAL', 'Cuti Tahunan', 12, true, false, true),
  ('LT_SICK', 'Cuti Sakit', 365, true, true, true)
on conflict (leave_type_id) do nothing;

create index if not exists idx_attendance_employee_tanggal on attendance(employee_id, tanggal);
create index if not exists idx_attendance_email_tanggal on attendance(email, tanggal);
create index if not exists idx_leave_employee_status on leave_requests(employee_id, status);
create index if not exists idx_payroll_employee_uploaded on payroll_docs(employee_id, uploaded_at);
create index if not exists idx_announcements_active_publish on announcements(is_active, published_at);
