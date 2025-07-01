CREATE TABLE IF NOT EXISTS "DS".FT_BALANCE_F (
  oper_date DATE NOT NULL,
  credit_account_rk NUMERIC NOT NULL
  debut_account_rk NUMERIC NOT NULL,
  credit_amount FLOAT,
  debet_amount FLOAT,
  PRIMARY KEY (on_date, account_rk)
);


CREATE TABLE IF NOT EXISTS "DS".FT_POSTING_F (
  oper_date DATE NOT NULL,
  credit_account_rk NUMERIC NOT NULL
  debut_account_rk NUMERIC NOT NULL,
  credit_amount FLOAT,
  debet_amount FLOAT
);

CREATE TABLE IF NOT EXISTS "DS".MD_ACCOUNT_D (
  data_actual_date DATE NOT NULL,
  data_actual_end_date DATE NOT NULL,
  account_rk NUMERIC NOT NULL,
  account_number VARCHAR(20) NOT NULL,
  char_type VARCHAR(1) NOT NULL,
  currency_rk NUMERIC NOT NULL,
  currency_code VARCHAR(3) NOT NULL,
  PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS "DS".MD_CURRENCY_D (
  currency_rk NUMERIC NOT NULL,
  data_actual_date DATE NOT NULL,
  data_actual_end_date DATE,
  currency_code VARCHAR(3),
  code_iso_char VARCHAR(3),
  PRIMARY KEY (currency_rk, data_actual_date)
)

CREATE TABLE IF NOT EXISTS "DS".MD_EXCHANGE_RATE_D (
  data_actual_date DATE NOT NULL,
  data_actual_end_date DATE,
  currency_rk NUMERIC NOT NULL,
  reduced_cource FLOAT,
  code_iso_num VARCHAR(3),
  PRIMARY KEY (data_actual_date, currency_rk)
)

CREATE TABLE IF NOT EXISTS "DS".MD_LEDGER_ACCOUNT_S (
  chapter CHAR(1),
  chapter_name VARCHAR(16),
  section_number INT,
  section_name VARCHAR(22),
  subsection_name VARCHAR(21),
  ledger1_account INT,
  ledger1_account_name VARCHAR(47),
  ledger_account INT NOT NULL,
  ledger_account_name VARCHAR(153),
  characteristic CHAR(1),
  is_resident INT,
  is_reserve INT,
  is_reserve INT,
  is_loan INT,
  is_reserved_assets INT,
  is_overdue INT,
  is_interest INT,
  pair_account VARCHAR(5),
  start_date DATE NOT NULL,
  end_date DATE,
  is_rub_only INT,
  main_term VARCHAR(1),
  main_term_measure VARCHAR(1),
  max_term VARCHAR(1),
  max_term_measure VARCHAR(1),
  ledger_acc_full_name_transmit VARCHAR(1)
  is_revaluation VARCHAR(1),
  is_correct VARCHAR(1),
  PRIMARY KEY (ledger_account, start_date)
);
