CREATE USER ds_owner   PASSWORD 'Kot1234!';
CREATE USER logs_owner PASSWORD 'Kot1234!';

CREATE SCHEMA DS   AUTHORIZATION ds_owner;
CREATE SCHEMA LOGS AUTHORIZATION logs_owner;

CREATE TABLE LOGS.etl_audit (
    run_id          BIGSERIAL PRIMARY KEY,
    job_name        text      NOT NULL,
    status          text      NOT NULL CHECK (status IN ('START','END','ERROR')),
    rows_processed  bigint,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    message         text
);

