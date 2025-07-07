-- === 1. Доступ к схемам ===
GRANT USAGE ON SCHEMA ds TO ds_owner, logs_owner;
GRANT USAGE ON SCHEMA logs TO ds_owner, logs_owner;

-- === 2. Все права на все таблицы ===
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ds TO ds_owner, logs_owner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA logs TO ds_owner, logs_owner;

-- === 3. Все права на все sequences ===
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ds TO ds_owner, logs_owner;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA logs TO ds_owner, logs_owner;

-- === 4. Все права на все функции (если есть, полезно для процедур) ===
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA ds TO ds_owner, logs_owner;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA logs TO ds_owner, logs_owner;

-- === 5. Default-права на будущие объекты (если кто-то создаёт от postgres) ===
ALTER DEFAULT PRIVILEGES IN SCHEMA ds
  GRANT ALL PRIVILEGES ON TABLES TO ds_owner, logs_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA logs
  GRANT ALL PRIVILEGES ON TABLES TO ds_owner, logs_owner;

ALTER DEFAULT PRIVILEGES IN SCHEMA ds
  GRANT ALL PRIVILEGES ON SEQUENCES TO ds_owner, logs_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA logs
  GRANT ALL PRIVILEGES ON SEQUENCES TO ds_owner, logs_owner;

ALTER DEFAULT PRIVILEGES IN SCHEMA ds
  GRANT ALL PRIVILEGES ON FUNCTIONS TO ds_owner, logs_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA logs
  GRANT ALL PRIVILEGES ON FUNCTIONS TO ds_owner, logs_owner;
