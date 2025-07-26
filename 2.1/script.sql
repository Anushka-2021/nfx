DELETE FROM dm.client 
WHERE client.ctid NOT IN(	
	SELECT MIN(ctid)
	FROM dm.client
	GROUP BY client_rk, effective_from_date
);
