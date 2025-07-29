-- корректное значение - account_out_sum предыдущего дня
SELECT *,
	CASE 
	WHEN LAG(account_out_sum) OVER(PARTITION BY account_rk ORDER BY effective_date ASC) <> account_in_sum
		THEN LAG(account_out_sum) OVER(PARTITION BY account_rk ORDER BY effective_date ASC)
	ELSE account_in_sum
	END 
	AS correct_account_in_sum
FROM dm.account_balance_turnover
ORDER BY account_rk, effective_date ASC;

-- корректное значение - account_in_sum текущего дня

SELECT *,
	CASE 
	WHEN LEAD(account_in_sum) OVER(PARTITION BY account_rk ORDER BY effective_date ASC) <> account_out_sum
		THEN LEAD(account_in_sum) OVER(PARTITION BY account_rk ORDER BY effective_date ASC)
	ELSE account_out_sum
	END 
	AS correct_account_out_sum
FROM dm.account_balance_turnover
ORDER BY account_rk, effective_date ASC;

-- либо

SELECT *,
	CASE 
	WHEN LAG(account_in_sum) OVER(PARTITION BY account_rk ORDER BY effective_date DESC) <> account_out_sum
		THEN LAG(account_in_sum) OVER(PARTITION BY account_rk ORDER BY effective_date DESC)
	ELSE account_out_sum
	END 
	AS correct_account_out_sum
FROM dm.account_balance_turnover
ORDER BY account_rk, effective_date DESC;
