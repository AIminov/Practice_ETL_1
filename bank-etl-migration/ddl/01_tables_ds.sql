/* ---------- факты ---------- */
CREATE TABLE DS.ft_balance_f (
    on_date       date      NOT NULL,
    account_rk    numeric   NOT NULL,
    currency_rk   numeric,
    balance_out   double precision,
    PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE DS.ft_posting_f (
    oper_date           date      NOT NULL,
    credit_account_rk   numeric   NOT NULL,
    debet_account_rk    numeric   NOT NULL,
    credit_amount       double precision,
    debet_amount        double precision
);
/* TRUNCATE-LOAD: первичный ключ не нужен */

/* ---------- измерения / справочники ---------- */
CREATE TABLE DS.md_account_d (
    data_actual_date    date      NOT NULL,
    data_actual_end_date date     NOT NULL,
    account_rk          numeric   NOT NULL,
    account_number      varchar(20)  NOT NULL,
    char_type           char(1)   NOT NULL,
    currency_rk         numeric   NOT NULL,
    currency_code       char(3)   NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE DS.md_currency_d (
    currency_rk         numeric   NOT NULL,
    data_actual_date    date      NOT NULL,
    data_actual_end_date date,
    currency_code       char(3),
    code_iso_char       char(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE DS.md_exchange_rate_d (
    data_actual_date    date      NOT NULL,
    data_actual_end_date date,
    currency_rk         numeric   NOT NULL,
    reduced_cource      double precision,
    code_iso_num        char(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE DS.md_ledger_account_s (
    chapter                     char(1),
    chapter_name                varchar(16),
    section_number              integer,
    section_name                varchar(22),
    subsection_name             varchar(21),
    ledger1_account             integer,
    ledger1_account_name        varchar(47),
    ledger_account              integer     NOT NULL,
    ledger_account_name         varchar(153),
    characteristic              char(1),
    is_resident                 integer,
    is_reserve                  integer,
    is_reserved                 integer,
    is_loan                     integer,
    is_reserved_assets          integer,
    is_overdue                  integer,
    is_interest                 integer,
    pair_account                varchar(5),
    start_date                  date        NOT NULL,
    end_date                    date,
    is_rub_only                 integer,
    min_term                    varchar(1),
    min_term_measure            varchar(1),
    max_term                    varchar(1),
    max_term_measure            varchar(1),
    ledger_acc_full_name_translit varchar(1),
    is_revaluation              varchar(1),
    is_correct                  varchar(1),
    PRIMARY KEY (ledger_account, start_date)
);
