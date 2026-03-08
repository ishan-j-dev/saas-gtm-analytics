-- ============================================================
-- 5. EXECUTIVE SUMMARY
-- ============================================================
SELECT
    -- Revenue
    (SELECT SUM(monthly_price)
     FROM cleaned_subscriptions
     WHERE status = 'active') AS current_mrr,
    (SELECT SUM(monthly_price) * 12
     FROM cleaned_subscriptions
     WHERE status = 'active') AS current_arr,
    -- Customers
    (SELECT COUNT(DISTINCT customer_id)
     FROM cleaned_subscriptions
     WHERE status = 'active') AS active_customers,
    (SELECT COUNT(DISTINCT customer_id)
     FROM cleaned_subscriptions
     WHERE status = 'canceled') AS total_churned_customers,
    -- Funnel
    (SELECT COUNT(DISTINCT customer_id)
     FROM cleaned_events
     WHERE event_type = 'trial_start') AS total_trials,
    (SELECT COUNT(DISTINCT customer_id)
     FROM cleaned_events
     WHERE event_type = 'activated') AS total_activated,
    -- Key rates
    ROUND((SELECT COUNT(DISTINCT customer_id)
         FROM cleaned_events WHERE event_type = 'trial_start')* 100.0 / 1000, 1) AS signup_to_trial_pct,
    ROUND((SELECT COUNT(DISTINCT customer_id)
         FROM cleaned_events WHERE event_type = 'activated')* 100.0 /(SELECT COUNT(DISTINCT customer_id)FROM cleaned_events WHERE event_type = 'trial_start'), 1) 
         AS trial_to_activated_pct,
    -- MRR at risk (paying but never activated)
    (SELECT SUM(monthly_price)FROM cleaned_subscriptions
     WHERE status = 'active'
	AND customer_id NOT IN (SELECT DISTINCT customer_id FROM cleaned_events WHERE event_type = 'activated')) AS mrr_at_risk;
    
-- ============================================================
-- 5.1  GROWTH BOTTLENECK
-- ============================================================

WITH RECURSIVE month_spine AS (
    SELECT DATE('2023-01-01') AS month_start
    UNION ALL
    SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
    FROM month_spine
    WHERE month_start < DATE('2023-12-01')
),
new_customers AS (
    SELECT
        DATE(DATE_FORMAT(start_date, '%Y-%m-01')) AS month_start,
        COUNT(DISTINCT customer_id) AS new_customers
    FROM cleaned_subscriptions
    GROUP BY month_start
),
lost_customers AS (
    SELECT
        DATE(DATE_FORMAT(end_date, '%Y-%m-01')) AS month_start,
        COUNT(DISTINCT customer_id) AS lost_customers
    FROM cleaned_subscriptions
    WHERE status = 'canceled' AND end_date IS NOT NULL
    GROUP BY month_start
)
SELECT
    DATE_FORMAT(ms.month_start, '%Y-%m') AS month,
    COALESCE(n.new_customers,  0) AS new_customers,
    COALESCE(l.lost_customers, 0) AS lost_customers,
    COALESCE(n.new_customers, 0)- COALESCE(l.lost_customers,0)AS net_customers,
    -- Running total of active customers
    SUM(COALESCE(n.new_customers,0)-COALESCE(l.lost_customers, 0)) OVER (ORDER BY ms.month_start) AS cumulative_customers
FROM month_spine ms
LEFT JOIN new_customers n ON ms.month_start = n.month_start
LEFT JOIN lost_customers l ON ms.month_start = l.month_start
ORDER BY ms.month_start;

-- ============================================================
-- 5.3  CHANNEL EFFICIENCY SCORE
-- ============================================================

WITH customer_source AS (
    SELECT customer_id, source
    FROM cleaned_events
    WHERE event_type = 'signup'
      AND event_id = (
          SELECT MIN(e2.event_id) 
          FROM cleaned_events e2
          WHERE e2.customer_id = cleaned_events.customer_id
            AND e2.event_type = 'signup'
      )
),
channel_metrics AS (
    SELECT
        COALESCE(cs.source, 'unknown') AS source,
        COUNT(DISTINCT c.customer_id) AS signups,
        COUNT(DISTINCT s.customer_id) AS paid_customers,
        COUNT(DISTINCT CASE WHEN s.status = 'canceled' THEN s.customer_id END) AS churned_customers,
        SUM(s.monthly_price) AS total_mrr,
        ROUND(AVG(s.monthly_price), 2) AS arpc
    FROM cleaned_customers c
    LEFT JOIN customer_source cs ON c.customer_id = cs.customer_id
    LEFT JOIN cleaned_subscriptions s ON c.customer_id = s.customer_id
    GROUP BY cs.source
)
SELECT
    source,
    signups,
    paid_customers,
    churned_customers,
    arpc,
    -- Conversion rate
    ROUND(paid_customers  * 100.0 / NULLIF(signups, 0), 1) AS conversion_pct,
    -- Churn rate for this channel
    ROUND(churned_customers * 100.0 / NULLIF(paid_customers, 0), 1) AS churn_pct,
    -- Retention rate (inverse of churn)
    ROUND((1 - churned_customers * 1.0 / NULLIF(paid_customers, 0)) * 100, 1) AS retention_pct,
    -- Efficiency score = conversion% × retention% / 100
    -- Higher = better quality channel
    ROUND((paid_customers * 1.0 / NULLIF(signups, 0))* (1 - churned_customers * 1.0 / NULLIF(paid_customers, 0))* 100, 1) AS efficiency_score
FROM channel_metrics
ORDER BY efficiency_score DESC;

/*
EFFICIENCY SCORE INTERPRETATION:
    Highest score = best ROI channel (converts well AND retains well)
    ads→ lowest churn (41.2%) = best retention
    referral → highest conversion (92.6%) but worst churn (48.1%)
    RECOMMENDATION: Scale ads spend. Investigate why referral customers churn fastest.
*/

-- ============================================================
-- 5.4  CHURN RISK — at-risk customer list
-- ============================================================

SELECT
    s.customer_id,
    COALESCE(c.segment, 'Not Provided') AS segment,
    c.country,
    s.monthly_price,
    s.start_date,
    DATEDIFF(CURDATE(), s.start_date) AS days_active,

    -- Risk Flag 1: Never activated (no product value moment)
    CASE WHEN s.customer_id NOT IN (
        SELECT DISTINCT customer_id FROM cleaned_events
        WHERE event_type = 'activated'
    ) THEN 1 ELSE 0 END AS no_activation,

    -- Risk Flag 2: On lowest price tier (low commitment signal)
    CASE WHEN s.monthly_price = 49 THEN 1 ELSE 0 END AS lowest_tier,

    -- Risk Flag 3: Active 60+ days with no activation event
    CASE WHEN DATEDIFF(CURDATE(), s.start_date) > 60
          AND s.customer_id NOT IN (
              SELECT DISTINCT customer_id FROM cleaned_events
              WHERE event_type = 'activated'
          )
    THEN 1 ELSE 0 END AS long_inactive,

-- Total risk score
    CASE WHEN s.customer_id NOT IN (
            SELECT DISTINCT customer_id FROM cleaned_events
            WHERE event_type = 'activated') THEN 1 ELSE 0 END
    + CASE WHEN s.monthly_price = 49 THEN 1 ELSE 0 END
    + CASE WHEN DATEDIFF(CURDATE(), s.start_date) > 60
           AND s.customer_id NOT IN (
               SELECT DISTINCT customer_id FROM cleaned_events
               WHERE event_type = 'activated') THEN 1 ELSE 0 END AS risk_score
FROM cleaned_subscriptions s
JOIN cleaned_customers c ON s.customer_id = c.customer_id
WHERE s.status = 'active'
ORDER BY risk_score DESC, s.monthly_price DESC;

-- Summary: how much MRR is at each risk level?
SELECT
    CASE WHEN s.customer_id NOT IN (
            SELECT DISTINCT customer_id FROM cleaned_events
            WHERE event_type = 'activated') THEN 1 ELSE 0 END 
            + 
            CASE WHEN s.monthly_price = 49 THEN 1 ELSE 0 END 
            + 
            CASE WHEN DATEDIFF(CURDATE(), s.start_date) > 60 
            AND s.customer_id NOT IN (
               SELECT DISTINCT customer_id FROM cleaned_events
               WHERE event_type = 'activated') THEN 1 ELSE 0 END AS risk_score,
    COUNT(*) AS customers,
    SUM(s.monthly_price) AS mrr_at_risk
FROM cleaned_subscriptions s
WHERE s.status = 'active'
GROUP BY risk_score
ORDER BY risk_score DESC;

-- ============================================================
-- 5.5  ESTIMATED LTV BY SEGMENT
-- ============================================================
-- LTV = ARPC / Monthly Churn Rate
-- WHICH SEGMENT IS WORTH THE MOST OVER TIME?
-- MORE LTV = WORTH TO SPEND MORE TO ACQUIRE

WITH segment_metrics AS (
    SELECT
        COALESCE(c.segment, 'Not Provided') AS segment,
        COUNT(DISTINCT s.customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN s.status = 'active' THEN s.customer_id END) AS active_customers,
        COUNT(DISTINCT CASE WHEN s.status = 'canceled' THEN s.customer_id END) AS churned_customers,
        ROUND(AVG(s.monthly_price), 2) AS arpc,
        ROUND(COUNT(DISTINCT CASE WHEN s.status='canceled' THEN s.customer_id END)* 1.0 
        / 
        NULLIF(COUNT(DISTINCT s.customer_id), 0), 4) AS churn_rate
    FROM cleaned_subscriptions s
    JOIN cleaned_customers c ON s.customer_id = c.customer_id
    GROUP BY c.segment
)
SELECT
    segment,
    total_customers,
    active_customers,
    churned_customers,
    arpc,
    ROUND(churn_rate * 100, 2) AS churn_rate_pct,
    -- LTV = ARPC / monthly churn rate
    CASE
        WHEN churn_rate > 0
        THEN ROUND(arpc / churn_rate, 0)
        ELSE NULL
    END AS estimated_ltv,
    -- Months of expected customer life = 1 / churn rate
    CASE
        WHEN churn_rate > 0
        THEN ROUND(1 / churn_rate, 1)
        ELSE NULL
    END AS avg_months_retained
FROM segment_metrics
ORDER BY estimated_ltv DESC;
