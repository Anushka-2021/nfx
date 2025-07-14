CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
BEGIN
DECLARE @log_id INT (INSERT INTO "LOGS".log_table (
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
	) RETURNING id);
	
DELETE FROM "DM".dm_account_turnover_f  WHERE on_date = i_OnDate;
INSERT INTO "DM".dm_account_turnover_f 
	SELECT 
		i_OnDate, 
		ftb.account_rk, 
		SUM(COALESCE(ftp_c.credit_amount, 0))::numeric AS credit_amount, 
		SUM(COALESCE(ftp_c.credit_amount, 0)*COALESCE(mer.reduced_cource, 1))::numeric AS credit_amount_rub,
		SUM(COALESCE(ftp_d.debet_amount, 0))::numeric AS debet_amount, 
		SUM(COALESCE(ftp_d.debet_amount, 0)*COALESCE(mer.reduced_cource, 1))::numeric AS debet_amount_rub
		
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
UPDATE "LOGS".log_table SET end_timestamp  = NOW, duration = NOW()-start_timestamp WHERE id = @log_id;

END;
$$ 
;

DO $$
	DECLARE cnt DATE;
BEGIN
    FOR cnt IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day')::date
 LOOP
	CALL "DS".fill_account_turnover_f(cnt);
    END LOOP;
END; $$



CREATE OR REPLACE PROCEDURE "DS".fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
BEGIN

DECLARE @log_id Int (INSERT INTO "LOGS".log_table (
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
	) RETURNING id);

DELETE FROM "DM".dm_account_balance_f WHERE on_date = i_onDate;

WITH 
	t_b AS (
		SELECT DISTINCT 
		fbf.on_date, 
		fbf.account_rk, 
		mad.char_type,
		COALESCE(merd.reduced_cource, 1),
		COALESCE(LAG(fbf.balance_out) OVER(
			PARTITION BY fbf.account_rk 
			ORDER BY fbf.on_date
		), 0) AS balance_prev,
		COALESCE(LAG(fbf.balance_out) OVER(
			PARTITION BY fbf.account_rk 
			ORDER BY fbf.on_date
		), 0)*COALESCE(merd.reduced_cource, 1) AS balance_prev_rub
			
		FROM "DS".ft_balance_f fbf
		RIGHT JOIN "DS".md_account_d mad ON(
			mad.account_rk = fbf.account_rk AND 
			i_OnDate BETWEEN mad.data_actual_date
				AND mad.data_actual_end_date+'1 day'::interval
		)
		LEFT JOIN "DS".md_exchange_rate_d merd ON(
			merd.currency_rk = mad.currency_rk
				AND (i_OnDate BETWEEN merd.data_actual_date
					AND merd.data_actual_end_date)
			)
		GROUP BY fbf.account_rk, fbf.on_date, mad.char_type, merd.reduced_cource
		ORDER BY fbf.account_rk, fbf.on_date
	),
	t_l AS (
		SELECT on_date, account_rk,
			credit_amount,
			credit_amount_rub,
			debet_amount,
			debet_amount_rub
		FROM "DM".dm_account_turnover_f
	)
	
	INSERT INTO "DM".dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)

		SELECT t_b.on_date,
				t_b.account_rk, 
				CASE 
					WHEN char_type = 'А' 
						THEN balance_prev+COALESCE(debet_amount, 0)-COALESCE(credit_amount, 0)
					ELSE balance_prev-COALESCE(debet_amount, 0)+COALESCE(credit_amount, 0)
				END AS balance_out,
				CASE 
					WHEN char_type = 'А' 
						THEN balance_prev_rub+COALESCE(debet_amount_rub, 0)-COALESCE(credit_amount_rub, 0)
					ELSE balance_prev_rub-COALESCE(debet_amount_rub, 0)+COALESCE(credit_amount_rub, 0)
				END AS balance_out_rub
				
		FROM t_b
		LEFT JOIN t_l ON(
			t_b.account_rk = t_l.account_rk
				AND t_b.on_date = t_l.on_date
		);
	
UPDATE "LOGS".log_table SET end_timestamp  = NOW, duration = NOW()-start_timestamp WHERE id = @log_id;

END;
$$ 
;

DO $$
	DECLARE cnt DATE;
BEGIN
    FOR cnt IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day')::date
 LOOP
	CALL "DS".fill_account_balance_f(cnt);
    END LOOP;
END; $$
