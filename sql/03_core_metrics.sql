-- ============================================================================================================================================================
-- ============================================================={[SaaS Metrics]}===================================================================================
-- ============================================================================================================================================================

/* MONTH SPINE CALCULATION SO THAT MONTHS WITH ZERO ACTIVITY DON'T VANISH FROM THE RESULTS. 
ADDITIONALLY THIS GENERATES ONE ROW PER MONTH FOR A FULL ANALYSIS PERIOD FOR EACH CUSTOMER(Jan-Dec 2023)
Without it, months with zero new signups, zero churn, or zero MRR movement would simply not appear in results — creating invisible gaps in trend charts.*/
WITH RECURSIVE month_spine AS
(
	SELECT 
		DATE('2023-01-01') AS month_start
		UNION ALL
		SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
		FROM month_spine
		WHERE month_start < DATE ('2023-12-01')
),
active_subs AS(SELECT ms.month_start, s.customer_id, s.monthly_price
FROM month_spine ms
JOIN cleaned_subscriptions s ON s.start_date <= LAST_DAY(ms.month_start) 
AND (s.end_date >= ms.month_start OR s.end_date IS NULL)
)
-- MRR, ARR, ARPC
SELECT
    DATE_FORMAT(month_start, '%Y-%m') AS month,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(monthly_price) AS mrr,
    SUM(monthly_price) * 12 AS arr,
    ROUND(SUM(monthly_price)/ COUNT(DISTINCT customer_id), 2) AS arpc
FROM active_subs
GROUP BY month_start
ORDER BY month_start;
-- =======================
-- ARPC BY SEGMENT
-- ======================
SELECT
		COALESCE(c.segment, 'Not Provided') AS segment,
		c.segment_imputed,
		COUNT(DISTINCT s.customer_id) AS paying_customers,
		SUM(s.monthly_price) AS segment_mrr,
		ROUND(AVG(s.monthly_price), 2) AS arpc,
		ROUND
			(SUM(s.monthly_price) * 100.0 /(SELECT SUM(monthly_price)
			FROM cleaned_subscriptions 
			WHERE status = 'active'), 1)  AS mrr_share_pct
FROM cleaned_subscriptions s
JOIN cleaned_customers c ON s.customer_id = c.customer_id
WHERE s.status = 'active'
GROUP BY c.segment, c.segment_imputed
ORDER BY segment_mrr DESC;
-- > 60 % customers from imputed segemnt still active.

-- NET NEW MRR EVERY MONTH
WITH RECURSIVE month_spine AS (
    SELECT DATE('2023-01-01') AS month_start
    UNION ALL
    SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
    FROM month_spine
    WHERE month_start < DATE('2023-12-01')
),
new_mrr AS (
    SELECT
        DATE(DATE_FORMAT(start_date, '%Y-%m-01')) AS month_start,
        SUM(monthly_price) AS new_mrr
    FROM cleaned_subscriptions
    GROUP BY month_start
),
churned_mrr AS (
    SELECT
        DATE(DATE_FORMAT(end_date, '%Y-%m-01')) AS month_start,
        SUM(monthly_price) AS churned_mrr
    FROM cleaned_subscriptions
    WHERE status = 'canceled' AND end_date IS NOT NULL
    GROUP BY month_start
)
SELECT
    DATE_FORMAT(ms.month_start, '%Y-%m') AS month,
    COALESCE(n.new_mrr, 0) AS new_mrr,
    COALESCE(c.churned_mrr, 0) AS churned_mrr,
    COALESCE(n.new_mrr, 0) - COALESCE(c.churned_mrr, 0) AS net_new_mrr
FROM month_spine ms
LEFT JOIN new_mrr n ON ms.month_start = n.month_start
LEFT JOIN churned_mrr c ON ms.month_start = c.month_start
ORDER BY ms.month_start;

/* "New customer acquisition completely stopped after April 2023 while churn continued through July, resulting in a 16% decline from peak MRR. 
This suggests either a go-to-market pause, a seasonal pattern, or a data collection cutoff. No net new MRR was recorded in the final 5 months of the dataset." */

-- /////////////////////////////////////////////////////////////////
-- CUSTOMER(LOGO) CHURN RATE
-- /////////////////////////////////////////////////////////////////
WITH RECURSIVE month_spine AS (
    SELECT DATE('2023-01-01') AS month_start
    UNION ALL
    SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
    FROM month_spine
    WHERE month_start < DATE('2023-12-01')
),
customers_bom AS (
    SELECT
        ms.month_start,
        COUNT(DISTINCT s.customer_id) AS bom_count
    FROM month_spine ms
    JOIN cleaned_subscriptions s
        ON  s.start_date < ms.month_start
        AND (s.end_date >= ms.month_start OR s.end_date IS NULL)
    GROUP BY ms.month_start
),
customers_churned AS (
    SELECT
        DATE(DATE_FORMAT(churn_month, '%Y-%m-01'))  AS month_start,
        COUNT(DISTINCT customer_id) AS churned_count
    FROM (
        SELECT customer_id,
		DATE_FORMAT(end_date, '%Y-%m-01') AS churn_month
        FROM cleaned_subscriptions
        WHERE status = 'canceled' AND end_date IS NOT NULL
        UNION
        SELECT customer_id,
		DATE_FORMAT(event_date, '%Y-%m-01')
        FROM cleaned_events
        WHERE event_type = 'churned'
    ) combined
    GROUP BY churn_month
)
SELECT
    DATE_FORMAT(b.month_start, '%Y-%m') AS month,
    b.bom_count AS customers_at_start,
    COALESCE(c.churned_count, 0)  AS churned,
    ROUND(COALESCE(c.churned_count, 0) * 100.0/ NULLIF(b.bom_count, 0), 2)  AS logo_churn_pct
FROM customers_bom b
LEFT JOIN customers_churned c ON b.month_start = c.month_start
ORDER BY b.month_start;

-- /////////////////////////////////////////////////////////////////////////////////////
--  REVENUE CHURN
-- /////////////////////////////////////////////////////////////////////////////////////
WITH RECURSIVE month_spine AS (
    SELECT DATE('2023-01-01') AS month_start
    UNION ALL
    SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
    FROM month_spine
    WHERE month_start < DATE('2023-12-01')
),
mrr_bom AS (
    SELECT
        ms.month_start,
        SUM(s.monthly_price) AS mrr_at_start
    FROM month_spine ms
    JOIN cleaned_subscriptions s
        ON  s.start_date < ms.month_start
        AND (s.end_date >= ms.month_start OR s.end_date IS NULL)
    GROUP BY ms.month_start
),
mrr_lost AS (
    SELECT
        DATE(DATE_FORMAT(end_date, '%Y-%m-01')) AS month_start,
        SUM(monthly_price) AS mrr_churned
    FROM cleaned_subscriptions
    WHERE status = 'canceled' AND end_date IS NOT NULL
    GROUP BY month_start
)
SELECT
    DATE_FORMAT(b.month_start, '%Y-%m') AS month,
    b.mrr_at_start,
    COALESCE(l.mrr_churned, 0) AS mrr_lost,
    ROUND(COALESCE(l.mrr_churned, 0) * 100.0/ NULLIF(b.mrr_at_start, 0), 2) AS revenue_churn_pct
FROM mrr_bom b
LEFT JOIN mrr_lost l ON b.month_start = l.month_start
ORDER BY b.month_start;

 /*Finding 1 — Healthy churn composition: Revenue churn is consistently half of logo churn, indicating the business retains higher-value customers 
   while losing lower-priced accounts. Enterprise and mid-market customers appear stickier than SMB.
	
   Finding 2 — Critical churn volume: Monthly logo churn peaked at 18.45% in April. 
   This is a serious retention problem concentrated in Feb–May 2023, coinciding exactly with when new customer acquisition stopped.*/
    

