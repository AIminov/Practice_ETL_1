/* 1. разрешаем видеть объекты схемы DS */
GRANT USAGE ON SCHEMA ds TO ds_owner;

/* 2. полный доступ к таблицам, с которыми работает ETL */
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ds.ft_balance_f        TO ds_owner;
GRANT SELECT, INSERT, DELETE              ON TABLE ds.ft_posting_f   TO ds_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ds.md_account_d        TO ds_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ds.md_currency_d       TO ds_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ds.md_exchange_rate_d  TO ds_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ds.md_ledger_account_s TO ds_owner;

/* 3. если появятся новые таблицы в DS — выдаём права автоматически */
ALTER DEFAULT PRIVILEGES FOR ROLE ds_owner IN SCHEMA ds
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ds_owner;
