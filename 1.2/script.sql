-- fill "DM".dm_account_balance_f for '2017-12-31'

INSERT INTO "DM".dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
	SELECT on_date ,
	account_rk, balance_out, 
	balance_out*COALESCE(mer. reduced_cource, 1) AS balance_out_rub
FROM "DS".ft_balance_f fbf
	LEFT JOIN "DS".md_exchange_rate_d mer ON(mer.currency_rk = fbf.currency_rk 
		AND ('2017-12-31' BETWEEN mer.data_actual_date AND mer.data_actual_end_date+'1 day'::interval))
WHERE on_date = '2017-12-31';
