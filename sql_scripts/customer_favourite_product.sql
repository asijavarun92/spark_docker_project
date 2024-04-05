WITH data as (SELECT custId, productSold, sum(unitsSold) as total_units_sold
FROM transactions
GROUP BY custId, productSold)

SELECT custId as customer_id, productSold as favourite_product FROM data
WHERE (custId, total_units_sold) in (
SELECT custid, max(total_units_sold) as total_units_sold
FROM data GROUP BY custid)