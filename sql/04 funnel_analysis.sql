-- FUNNEL ANALYSIS
-- FUNNEL: Signup → Trial → Activated → Paid → Churned
/*
--   906 customers have subscriptions (Paid stage).
--   Only 404 customers have an 'activated' event.
	 This means Activated→Paid conversion appears > 100%, which is impossible. 
     Root cause: activation events were not consistently logged — a tracking gap, not a business reality.
*/
-- ================================================================================================
-- OVERALL FUNNEL
-- =================================================================================================
WITH customer_flags AS(
    SELECT
        c.customer_id,
        1  AS did_signup,
        MAX(CASE WHEN e.event_type = 'trial_start' THEN 1 ELSE 0 END) AS did_trial,
        MAX(CASE WHEN e.event_type = 'activated' THEN 1 ELSE 0 END) AS did_activate,
        MAX(CASE WHEN s.subscription_id IS NOT NULL THEN 1 ELSE 0 END) AS did_pay,
        MAX(CASE WHEN s.status = 'canceled' THEN 1 WHEN e.event_type = 'churned' THEN 1 ELSE 0 END) AS did_churn
    FROM cleaned_customers c
    LEFT JOIN cleaned_events e ON c.customer_id = e.customer_id
    LEFT JOIN cleaned_subscriptions s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id
)
SELECT
    COUNT(*) AS total_customers,
    SUM(did_trial) AS reached_trial,
    SUM(did_activate)  AS reached_activated,
    SUM(did_pay)  AS reached_paid,
    SUM(did_churn) AS churned,
    -- Each rate = this stage as % of the PREVIOUS stage
    ROUND(SUM(did_trial) * 100.0 / COUNT(*),2) AS signup_trial_pct,
    ROUND(SUM(did_activate) * 100.0 / NULLIF(SUM(did_trial), 0), 2) AS trial_activated_pct,
    -- NOTE: activated_to_paid will exceed 100% due to tracking gap
    ROUND(SUM(did_pay) * 100.0 / NULLIF(SUM(did_activate), 0), 2) AS activated_paid_pct,
    ROUND(SUM(did_churn) * 100.0 / NULLIF(SUM(did_pay), 0), 1)  AS paid_churn_pct
FROM customer_flags;

-- ================================================================================================
-- DROP OFF SUMMARY
-- =================================================================================================
WITH funnel_flags AS (
    SELECT
        c.customer_id,
        MAX(CASE WHEN e.event_type = 'trial_start' THEN 1 ELSE 0 END) AS did_trial,
        MAX(CASE WHEN e.event_type = 'activated' THEN 1 ELSE 0 END) AS did_activate,
        MAX(CASE WHEN s.subscription_id IS NOT NULL THEN 1 ELSE 0 END) AS did_pay,
        MAX(CASE 
			WHEN s.status = 'canceled' THEN 1
            WHEN e.event_type = 'churned' THEN 1
            ELSE 0
        END) AS did_churn
    FROM cleaned_customers c
    LEFT JOIN cleaned_events e ON c.customer_id = e.customer_id
    LEFT JOIN cleaned_subscriptions s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id
),
totals AS (
    SELECT
        COUNT(*) AS signups,
        SUM(did_trial) AS trials,
        SUM(did_activate) AS activated,
        SUM(did_pay) AS paid,
        SUM(did_churn) AS churned
    FROM funnel_flags
)
SELECT 'Signup to Trial' AS transition, signups AS from_count, trials AS to_count, signups - trials AS lost, ROUND((signups - trials) * 100.0 / signups,             1) AS drop_off_pct FROM totals
UNION ALL
SELECT 'Trial to Activated', trials, activated, trials - activated, ROUND((trials - activated)    * 100.0 / NULLIF(trials,    0), 1) FROM totals
UNION ALL
SELECT 'Activated to Paid', activated, paid, activated - paid, ROUND((activated - paid) * 100.0 / NULLIF(activated, 0), 1) FROM totals
UNION ALL
SELECT 'Paid to Churned', paid, churned, paid - churned, ROUND(churned * 100.0 / NULLIF(paid, 0), 1) FROM totals;

-- Paid count (906) exceeds Activated count (404) because activation events are missing for 55% of paying customers, 
-- likely due to incomplete event tracking. This is a data quality finding, not a funnel logic error

-- ================================================================================================
-- FUNNEL BY ACQUISITION SOURCE
-- =================================================================================================
WITH customer_source AS (
    -- One row per customer: their signup source
    SELECT customer_id, source
    FROM cleaned_events
    WHERE event_type = 'signup'
),
customer_flags AS (
    SELECT
        c.customer_id,
        cs.source,
        MAX(CASE WHEN e.event_type = 'trial_start' THEN 1 ELSE 0 END) AS did_trial,
        MAX(CASE WHEN e.event_type = 'activated' THEN 1 ELSE 0 END) AS did_activate,
        MAX(CASE WHEN s.subscription_id IS NOT NULL THEN 1 ELSE 0 END) AS did_pay,
        MAX(CASE WHEN s.status = 'canceled' THEN 1 WHEN e.event_type = 'churned' THEN 1 ELSE 0 END) AS did_churn
    FROM cleaned_customers c
    LEFT JOIN customer_source cs ON c.customer_id = cs.customer_id
    LEFT JOIN cleaned_events e ON c.customer_id = e.customer_id
    LEFT JOIN cleaned_subscriptions s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id, cs.source
)
SELECT
    source,
    COUNT(*) AS signups,
    SUM(did_trial) AS trials,
    SUM(did_activate) AS activated,
    SUM(did_pay) AS paid,
    SUM(did_churn) AS churned,
    ROUND(SUM(did_trial) * 100.0 / COUNT(*), 1) AS signup_trial_pct,
    ROUND(SUM(did_pay) * 100.0 / COUNT(*), 1) AS signup_paid_pct,
    ROUND(SUM(did_churn) * 100.0 / NULLIF(SUM(did_pay), 0), 1) AS churn_rate_pct
FROM customer_flags
GROUP BY source
ORDER BY paid DESC;

-- ================================================================================================
-- FUNNEL BY Customer Segment
-- =================================================================================================
WITH funnel_flags AS (
    SELECT
        c.customer_id,
        COALESCE(c.segment, 'Not Provided') AS segment,
        MAX(CASE WHEN e.event_type = 'trial_start'  THEN 1 ELSE 0 END) AS did_trial,
        MAX(CASE WHEN e.event_type = 'activated'    THEN 1 ELSE 0 END) AS did_activate,
        MAX(CASE WHEN s.subscription_id IS NOT NULL THEN 1 ELSE 0 END) AS did_pay,
        MAX(CASE WHEN s.status='canceled' OR e.event_type='churned' THEN 1 ELSE 0 END) AS did_churn
    FROM cleaned_customers c
    LEFT JOIN cleaned_events e ON c.customer_id = e.customer_id
    LEFT JOIN cleaned_subscriptions s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id, c.segment
)
SELECT
    segment,
    COUNT(*) AS signups,
    SUM(did_trial) AS trials,
    SUM(did_activate) AS activated,
    SUM(did_pay) AS paid,
    ROUND(SUM(did_trial) * 100.0 / COUNT(*), 1) AS signup_to_trial_pct,
    ROUND(SUM(did_activate) * 100.0 / NULLIF(SUM(did_trial),0),1) AS trial_to_activated_pct,
    ROUND(SUM(did_pay) * 100.0 / COUNT(*), 1) AS signup_to_paid_pct,
    ROUND(SUM(did_churn) * 100.0 / NULLIF(SUM(did_pay),0), 1)  AS churn_rate_pct
FROM funnel_flags
GROUP BY segment
ORDER BY signup_to_paid_pct DESC;

-- ================================================================================================
-- PAID BUT NEVER ACTIVATED (at-risk customers)
-- =================================================================================================
SELECT
    COUNT(*) AS active_paying_no_activation,
    SUM(monthly_price) AS mrr_at_risk
FROM cleaned_subscriptions
WHERE status = 'active' AND customer_id NOT IN (
      SELECT DISTINCT customer_id
      FROM cleaned_events
      WHERE event_type = 'activated'
  );
