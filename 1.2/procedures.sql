-- PROCEDURE: DS.fill_account_turnover_f(date)

-- DROP PROCEDURE IF EXISTS "DS".fill_account_turnover_f(date);

CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f(
	IN i_ondate date)
LANGUAGE 'plpgsql'
AS $$

DECLARE log_id INT;
	
BEGIN

DELETE FROM "DM".dm_account_turnover_f  WHERE on_date = i_OnDate; 

INSERT INTO "LOGS".log_table (
	start_timestamp, 
	username, 
	"database",
	"action",
	"table"
	) VALUES (
	NOW(),
	current_user,
	'nfx_cp1',
	'insert',
	'"DM".dm_account_turnover_f'
	) RETURNING id INTO log_id;

INSERT INTO "DM".dm_account_turnover_f 
	SELECT 
		i_OnDate, 
		ftb.account_rk, 
		SUM(COALESCE(ftp_c.credit_amount, 0))::numeric AS credit_amount, 
		SUM(COALESCE(ftp_c.credit_amount, 0)*COALESCE(mer.reduced_cource, 1))::numeric 
			AS credit_amount_rub,
		SUM(COALESCE(ftp_d.debet_amount, 0))::numeric AS debet_amount, 
		SUM(COALESCE(ftp_d.debet_amount, 0)*COALESCE(mer.reduced_cource, 1))::numeric 
			AS debet_amount_rub
		
	FROM "DS".ft_balance_f ftb
	LEFT JOIN "DS".ft_posting_f ftp_c ON(ftb.account_rk = ftp_c.credit_account_rk)
	LEFT JOIN "DS".ft_posting_f ftp_d ON(ftb.account_rk = ftp_d.debet_account_rk)
	LEFT JOIN "DS".md_exchange_rate_d mer ON(mer.currency_rk = ftb.currency_rk)
	
	WHERE (ftp_c.oper_date = i_OnDate OR ftp_d.oper_date = i_OnDate) 
		AND (mer.data_actual_date <=i_OnDate
			AND (mer.data_actual_end_date >= i_OnDate
				OR mer.data_actual_end_date IS NULL
			)
		)
	GROUP BY on_date, account_rk;

UPDATE "LOGS".log_table SET end_timestamp  = NOW(), duration = NOW()-start_timestamp WHERE id = log_id;

END;
$$;



DO $$
	DECLARE cnt DATE;
BEGIN
    FOR cnt IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day')::date
 LOOP
	CALL "DS".fill_account_turnover_f(cnt);
    END LOOP;
END; $$


------------------------------------------------------------------

-- PROCEDURE: DS.fill_account_balance_f(date)

-- DROP PROCEDURE IF EXISTS "DS".fill_account_balance_f(date);

CREATE OR REPLACE PROCEDURE "DS".fill_account_balance_f(
	IN i_ondate date) LANGUAGE 'plpgsql' 
AS $$

DECLARE log_id INT; 

BEGIN 
DELETE FROM "DM".dm_account_balance_f WHERE on_date = i_onDate;
INSERT INTO "LOGS".log_table (
	start_timestamp, 
	username, 
	"database",
	"action",
	"table"
	) VALUES (
	NOW(),
	current_user,
	'nfx_cp1',
	'insert',
	'"DM".dm_account_balance_f'
	) RETURNING id INTO log_id;

WITH 
	t_b AS (
		SELECT  
			i_OnDate AS on_date,
			dabf.account_rk, 
			ROUND(COALESCE(dabf.balance_out, 0), 2) AS balance_out,
			ROUND(COALESCE(dabf.balance_out_rub, 0), 2) AS balance_out_rub, 
			mad.char_type
			
		FROM "DM".dm_account_balance_f dabf
		RIGHT JOIN "DS".md_account_d mad ON(
			mad.account_rk = dabf.account_rk AND 
			i_OnDate BETWEEN mad.data_actual_date
				AND mad.data_actual_end_date+'1 day'::interval
		)
		WHERE dabf.on_date = i_OnDate::date - '1 day'::interval
	),
	t_t AS (SELECT on_date, account_rk,
			ROUND(credit_amount, 2) AS credit_amount,
			ROUND(credit_amount_rub, 2) AS credit_amount_rub,
			ROUND(debet_amount, 2) AS debet_amount,
			ROUND(debet_amount_rub, 2) AS debet_amount_rub
		FROM "DM".dm_account_turnover_f
	WHERE on_date = i_OnDate
	)
	
INSERT INTO "DM".dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)

	SELECT
		t_b.on_date, 
		t_b.account_rk,
		CASE 
			WHEN t_b.char_type = 'А' 
				THEN t_b.balance_out+COALESCE(debet_amount, 0)-COALESCE(credit_amount, 0)
			ELSE t_b.balance_out-COALESCE(debet_amount, 0)+COALESCE(credit_amount, 0)
		END AS balance_out,
	CASE 
		WHEN char_type = 'А' 
			THEN t_b.balance_out_rub+COALESCE(debet_amount_rub, 0)-COALESCE(credit_amount_rub, 0)
		ELSE t_b.balance_out_rub-COALESCE(debet_amount_rub, 0)+COALESCE(credit_amount_rub, 0)
	END AS balance_out_rub
	
		FROM t_b
		LEFT JOIN t_t USING(account_rk);
	
UPDATE "LOGS".log_table SET end_timestamp  = NOW(), duration = NOW()-start_timestamp WHERE id = @log_id;

END;
$$;

DO $$
	DECLARE cnt DATE;
BEGIN
    FOR cnt IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day')::date
 LOOP
	CALL "DS".fill_account_balance_f(cnt);
    END LOOP;
END; $$
