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
  foto_masuk_url text,
  foto_keluar_url text,
  catatan text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

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

create index if not exists idx_attendance_employee_tanggal on attendance(employee_id, tanggal);
create index if not exists idx_attendance_email_tanggal on attendance(email, tanggal);
create index if not exists idx_leave_employee_status on leave_requests(employee_id, status);
