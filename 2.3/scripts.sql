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


-- скрипт 

WITH cor_in AS (SELECT account_rk,
	effective_date,
	CASE 
	WHEN LAG(account_out_sum) OVER(PARTITION BY account_rk ORDER BY effective_date ASC) <> account_in_sum
		THEN LAG(account_out_sum) OVER(PARTITION BY account_rk ORDER BY effective_date ASC)
	ELSE account_in_sum
	END AS account_in_sum, 
	account_out_sum
FROM rd.account_balance)

UPDATE rd.account_balance
	SET account_in_sum = (
		SELECT cor_in.account_in_sum
		FROM cor_in
		WHERE account_balance.account_rk = cor_in.account_rk AND account_balance.effective_date = cor_in.effective_date)


-- процедура
	
CREATE OR REPLACE PROCEDURE rd.fill_account_balance_turnover()
LANGUAGE 'plpgsql'
AS $BODY$

BEGIN

DELETE FROM dm.account_balance_turnover;

INSERT INTO dm.account_balance_turnover
	
	SELECT a.account_rk,
		   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
		   a.department_rk,
		   ab.effective_date,
		   ab.account_in_sum,
		   ab.account_out_sum
	FROM rd.account a
	LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
	LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
		
END;
$BODY$;
