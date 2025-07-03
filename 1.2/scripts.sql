CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f2(i_OnDate DATE)
LANGUAGE plpgsql AS $$
BEGIN
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
	GROUP BY on_date, account_rk
	ORDER BY 2 NULLS FIRST;
END;
$$ 
