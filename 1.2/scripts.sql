SELECT 
	on_date, 
	account_rk, 
	SUM(COALESCE(ftp_c.credit_amount, 0)) AS credit_amount, 
	SUM(ftp_c.credit_amount*COALESCE(mer.reduced_cource, 1)) AS credit_amount_rub,
	SUM(COALESCE(ftp_d.debet_amount, 0)) AS debet_amount, 
	SUM(ftp_d.debet_amount*COALESCE(mer.reduced_cource, 1)) AS debet_amount_rub
	
	
	FROM "DS".ft_balance_f ftb
	LEFT JOIN "DS".ft_posting_f ftp_c ON(ftb.account_rk = ftp_c.credit_account_rk)
	LEFT JOIN "DS".ft_posting_f ftp_d ON(ftb.account_rk = ftp_d.debet_account_rk)
	LEFT JOIN "DS".md_exchange_rate_d mer ON(mer.currency_rk = ftb.currency_rk)


WHERE ((ftp_c.oper_date = '2017-12-31'	OR ftp_d.oper_date = '2017-12-31') 
	OR (ftp_c.oper_date = '2018-01-09'	OR ftp_d.oper_date = '2018-01-09')
	)
	AND (mer.data_actual_date <='2017-12-31'
		AND (mer.data_actual_end_date >= '2017-12-31'
			OR mer.data_actual_end_date IS NULL
		)
	)
