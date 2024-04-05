WITH data as (
SELECT custId, transactionDate,
date_sub(transactionDate, (row_number() over (PARTITION BY custId  order by transactionDate))) as grp
FROM (SELECT distinct custId, transactionDate FROM transactions) t
)

SELECT custId as customer_id, max(streak) as longest_streak FROM (
SELECT custId, grp, count(transactionDate) as streak FROM data
group by custId, grp) a
group by custId