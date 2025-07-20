CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f(
	IN i_ondate date)
LANGUAGE 'plpgsql'
AS $$

DECLARE log_id INT;
	from_date DATE = i_OnDate - '1 month'::interval;
	end_date DATE = i_OnDate - '1 day'::interval;
	
BEGIN

DELETE FROM "DM".dm_f101_round_f 
	WHERE from_date = from_date AND end_date = end_date; 

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
	'"DM".dm_f101_round_f'
	) RETURNING id INTO log_id;

INSERT INTO "DM".dm_f101_round_f 
	SELECT 
		from_date,
		to_date,
		mlas.chapter,
		mlas.ledger_account,
		account_number,
		mad.char_type AS characteristic,
	
		CASE WHEN mad.currency_code IN('810', '643') 
			THEN ROUND(SUM(dabf_prev.balance_out_rub), 2)
		END AS balance_in_rub,
		CASE WHEN mad.currency_code NOT IN('810', '643') 
			THEN ROUND(SUM(dabf_prev.balance_out_rub), 2)
		END AS balance_in_val,
		ROUND(SUM(dabf_prev.balance_out_rub), 2) AS balance_in_total,
		
		CASE WHEN mad.currency_code IN('810', '643') 
			THEN ROUND(SUM(datf.debet_amount_rub), 2)
		END AS turn_deb_rub,
		CASE WHEN mad.currency_code NOT IN('810', '643') 
			THEN ROUND(SUM(datf.debet_amount_rub), 2)
		END AS turn_deb_val,
		ROUND(SUM(datf.debet_amount_rub), 2) AS turn_deb_total,
	
		CASE WHEN mad.currency_code IN('810', '643') 
			THEN ROUND(SUM(datf.credit_amount_rub), 2)
		END AS turn_cre_rub,
		CASE WHEN mad.currency_code NOT IN('810', '643') 
			THEN ROUND(SUM(datf.credit_amount_rub), 2)
		END AS turn_cre_val,
		ROUND(SUM(datf.credit_amount_rub), 2) AS turn_cre_total,
	
		CASE WHEN mad.currency_code IN('810', '643') 
			THEN ROUND(SUM(dabf.balance_out_rub), 2)
		END AS balance_out_rub,
		CASE WHEN mad.currency_code NOT IN('810', '643') 
			THEN ROUND(SUM(dabf.balance_out_rub), 2)
		END AS balance_out_val,
		ROUND(SUM(dabf.balance_out_rub), 2) AS balance_out_total
		
	FROM "DS".md_ledger_account_s mlas
		LEFT JOIN "DS".md_account_d mad 
			ON(SUBSTR(mad.account_number, 1, 5)::int = mlas.ledger_account)
		LEFT JOIN "DM".dm_account_balance_f dabf ON(dabf.account_rk = mad.account_rk
			AND dabf.on_date = end_date)
		LEFT JOIN "DM".dm_account_balance_f dabf_prev ON(dabf_prev.account_rk = mad.account_rk
			AND dabf_prev.on_date = (from_date -'1 day'::interval)::date)
		FULL JOIN "DM".dm_account_turnover_f datf ON(datf.account_rk = mad.account_rk
			AND datf.on_date BETWEEN from_date AND end_date)
	WHERE mlas.start_date <= end_date
		AND (mad.data_actual_end_date IS NULL OR mad.data_actual_end_date >= end_date)
	GROUP BY mlas.chapter,
		mlas.ledger_account,
		account_number,
		mad.char_type, mad.currency_code, mad.account_rk
	;

UPDATE "LOGS".log_table SET end_timestamp  = NOW(), duration = NOW()-start_timestamp WHERE id = log_id;

END;
$$;
