-- Migrasi prefix employee_id: EMP -> TGH
-- Aman dijalankan berulang (idempotent) untuk data yang sudah TGH.
-- Jalankan di Supabase SQL Editor pada project production.

begin;

-- 1) Guard: cegah konflik ID jika target TGH sudah ada.
do $$
declare
  v_conflict_count integer;
begin
  select count(*)
  into v_conflict_count
  from employees e_old
  join employees e_new
    on e_new.employee_id = regexp_replace(e_old.employee_id, '^EMP([_-])', 'TGH\1')
  where e_old.employee_id ~ '^EMP([_-])';

  if v_conflict_count > 0 then
    raise exception 'Migrasi dibatalkan: ditemukan konflik employee_id target TGH (% baris).', v_conflict_count;
  end if;
end $$;

-- 2) Ubah employee_id di tabel master.
-- FK attendance/leave_requests/payroll_docs sudah ON UPDATE CASCADE.
update employees
set employee_id = regexp_replace(employee_id, '^EMP([_-])', 'TGH\1')
where employee_id ~ '^EMP([_-])';

-- 3) Ubah key config yang menyimpan employee_id pada nama key.
with key_map as (
  select
    key as old_key,
    regexp_replace(key, '^PROFILE_EXTRA_EMP([_-])', 'PROFILE_EXTRA_TGH\1') as new_key
  from config
  where key ~ '^PROFILE_EXTRA_EMP([_-])'
  union all
  select
    key as old_key,
    regexp_replace(key, '^FACE_PROFILE_EMP([_-])', 'FACE_PROFILE_TGH\1') as new_key
  from config
  where key ~ '^FACE_PROFILE_EMP([_-])'
  union all
  select
    key as old_key,
    regexp_replace(key, '^AUTH_ACTIVATION_OUTBOX_EMP([_-])', 'AUTH_ACTIVATION_OUTBOX_TGH\1') as new_key
  from config
  where key ~ '^AUTH_ACTIVATION_OUTBOX_EMP([_-])'
  union all
  select
    key as old_key,
    regexp_replace(key, '^AUTH_EMAIL_AUDIT_EMP([_-])', 'AUTH_EMAIL_AUDIT_TGH\1') as new_key
  from config
  where key ~ '^AUTH_EMAIL_AUDIT_EMP([_-])'
  union all
  select
    key as old_key,
    regexp_replace(key, '^AUTH_RESET_RATE_EMP([_-])', 'AUTH_RESET_RATE_TGH\1') as new_key
  from config
  where key ~ '^AUTH_RESET_RATE_EMP([_-])'
),
dedup as (
  -- Jika key target sudah ada, hapus key lama agar tidak bentrok PK.
  delete from config c
  using key_map m
  where c.key = m.old_key
    and exists (select 1 from config x where x.key = m.new_key)
  returning c.key
)
update config c
set key = m.new_key
from key_map m
where c.key = m.old_key
  and not exists (select 1 from dedup d where d.key = c.key);

-- 4) Ubah employee_id yang tersimpan di JSON text config (session/face/outbox/audit).
update config
set value = regexp_replace(value, '"employee_id"\s*:\s*"EMP([_-])', '"employee_id":"TGH\1', 'g')
where value ~ '"employee_id"\s*:\s*"EMP([_-])';

-- 5) Laporan ringkas hasil migrasi.
do $$
declare
  v_emp_old integer;
  v_emp_new integer;
begin
  select count(*) into v_emp_old from employees where employee_id ~ '^EMP([_-])';
  select count(*) into v_emp_new from employees where employee_id ~ '^TGH([_-])';
  raise notice 'Sisa employee_id prefix EMP: %', v_emp_old;
  raise notice 'Total employee_id prefix TGH: %', v_emp_new;
end $$;

commit;

