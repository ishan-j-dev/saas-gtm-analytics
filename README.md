# Emergence Data Analyst Take-Home Assessment
**Role:** Data Analyst — SaaS Growth & GTM Analytics  
**Tools:** MySQL · Python · Power BI  
**Database:** saasgtm  
B2B SaaS growth analytics assessment — MySQL, Python, Power BI. Covers MRR, churn, funnel analysis, channel efficiency and LTV across 3 datasets.

---

## 1. Project Overview

This project analyzes three raw datasets from a B2B SaaS company to answer four business questions for leadership:

1. How is revenue performing over time?
2. Where are we losing customers in the funnel?
3. Which acquisition channels are most effective?
4. What is driving churn and how much revenue is at risk?

The data is intentionally messy. All issues were identified, documented, and handled before any analysis was run. Raw tables are preserved unchanged.

---

## 2. Repository Structure

```
data-analyst-assessment/
│
├── data/
│   ├── customers.csv
│   ├── subscriptions.csv
│   └── events.csv
│
├── sql/
│   ├── 01_raw_table_creation.sql
│   ├── 02_data_cleaning.sql
│   ├── 03_core_metrics.sql
│   ├── 04_funnel_analysis.sql
│   └── 05_optional_analysis.sql
│
├── python/
│   └── data_validation.py
│
├── dashboard/
│   ├── dashboard_page1.png   (Acquisition & Funnel)
│   ├── dashboard_page2.png   (Revenue & Churn)
│   ├── dashboard_page3.png   (Channel & Segment)
│   └── SaaS dashboard.pbix
│
└── README.md
```

---

## 3. Data Issues Identified & Handling

### 3.1 Raw Data Problems Found

| Issue | Table | Count | How Handled |
|---|---|---|---|
| Empty strings `''` instead of NULL | customers, subscriptions | Multiple columns | `NULLIF(TRIM(@var), '')` in LOAD DATA |
| Dates stored as text in CSV | All 3 tables | All date columns | Loaded as VARCHAR, cast via `STR_TO_DATE('%Y-%m-%d')` |
| Missing `signup_date` | customers | 36 rows (3.6%) | Kept as NULL; flagged; excluded from time-series queries |
| Missing `segment` | customers | 243 rows (24.3%) | Kept as NULL |
| `is_enterprise` as 'True'/'False' string | customers | All 1,000 rows | Converted to 1/0 via `CASE WHEN LOWER(TRIM(...))` |
| NULL `end_date` on active subscriptions | subscriptions | 718 rows | Expected behavior — NULL means still active |
| Customers with multiple subscriptions | subscriptions | 35 customers | All records kept; latest active used for snapshots |
| Duplicate events (same customer + type + date) | events | 188 rows (94 groups) | Deduplicated using `ROW_NUMBER()`, kept first event_id per group |

### 3.2 Why VARCHAR for Dates on Import

MySQL's `LOAD DATA` silently inserts `0000-00-00` if a date value doesn't match the expected format exactly without any warning or error. By loading all dates as VARCHAR first, we guarantee the raw data is preserved exactly as it arrived. Type conversion happens explicitly in the cleaning step using `STR_TO_DATE('%Y-%m-%d')`, which returns NULL on any format mismatch — making bad rows visible instead of hiding them.

### 3.3 The Activation Gap

906 customers have a subscription record(`paid`) but only 404 have an `activated` event. This means 55% of paying customers have no activation event logged. This is **not a funnel logic error** — it is a data quality finding. Activation events were likely not tracked consistently for all customers, particularly in early cohorts. This is documented as a limitation in all funnel queries.

---

## 4. Metric Definitions

| Metric | Definition | Formula |
|---|---|---|
| **MRR** | Monthly Recurring Revenue — total subscription value active in a given month | `SUM(monthly_price)` for all active subscriptions in month M |
| **ARR** | Annual Run Rate — annualized current revenue | `MRR × 12` |
| **ARPC** | Average Revenue Per Customer | `MRR ÷ active customers` |
| **Logo Churn Rate** | % of customers lost in a month | `Customers churned in M ÷ Customers at START of M × 100` |
| **Revenue Churn Rate** | % of MRR lost in a month | `MRR lost in M ÷ MRR at START of M × 100` |
| **LTV** | Estimated Customer Lifetime Value | `ARPC ÷ Monthly Churn Rate` |
| **Efficiency Score** | Channel quality metric (conversion × retention) | `(paid/signups) × (1 - churned/paid) × 100` |

**Why Beginning-of-Month (BOM) for churn denominator:**  
Industry standard. Using end-of-month inflates the denominator with new customers who joined that month, artificially deflating the churn rate. BOM gives a true picture of retention.

**Why UNION for churn signals:**  
Churned customers are identified via two signals: `status = 'canceled'` in subscriptions AND a `'churned'` event in events. UNION (not UNION ALL) deduplicates customers who appear in both, preventing double-counting.

---

## 5. Key Findings

### 5.1 Revenue Performance

| Metric | Value |
|---|---|
| Current MRR | 187,242 |
| Current ARR | 2,246,904 |
| Active Customers | 683 |
| Peak MRR (April 2023) | 223,455 |
| MRR decline from peak | -36,213 (-16.2%) |
| MRR at risk (unactivated paying customers) | **105,539** |

*Note: Currency is not specified in the source data. All monetary values are raw numeric units.*

**The growth-to-decline transition:**  
The business grew rapidly from January through March 2023, adding 319 net new customers in March alone. Growth stopped entirely after April. Churn continued through July. The dataset flatlines from August onward, consistent with a data collection cutoff rather than true business stabilization.

```
Jan–Mar 2023:  Pure growth phase  → +841 new subscriptions, $0 churn
Apr 2023:      Inflection point   → only 65 new, 80 churned, net -15
May–Jul 2023:  Bleed phase        → 0 new customers, continued churn
Aug–Dec 2023:  Flatline           → 683 customers, no movement (data ends)
```

### 5.2 Churn Analysis

| Month | Logo Churn | Revenue Churn | Signal |
|---|---|---|---|
| Feb 2023 | 12.35% | 5.88% | Logo 2× Revenue |
| Mar 2023 | 16.34% | 8.82% | Logo 2× Revenue |
| Apr 2023 | 18.45% | 9.71% | Worst month — peak churn |
| May 2023 | 16.45% | 5.83% | Logo 3× Revenue |
| Jun 2023 | 6.52% | 2.24% | Churn slowing |
| Jul 2023 | 1.61% | 0.04% | Near zero |

**Logo churn is consistently 2–3× revenue churn.**  
This means the business is losing its smaller/cheaper customers while retaining higher-value accounts. This is the healthier churn composition for a B2B SaaS company.

**However — the absolute churn rates are critically high.**  
Peak logo churn of 18.45% in April. This is a serious retention problem concentrated in the Feb–May 2023 window.

### 5.3 Funnel Analysis

```
Stage           Count    Conversion (vs previous stage)
─────────────────────────────────────────────────────
Signup          1,000    —
Trial             676    67.6%  (32.4% never started a trial)
Activated         404    59.8%  (40.2% drop-off ← BIGGEST GAP)
Paid              906    *see note
Churned           407    44.9% of paid
```

**Biggest drop-off: Trial → Activated at 40.2%**  
272 customers started a trial but never hit the activation milestone. This is the single largest funnel leak. It points to an onboarding problem — customers are interested enough to trial but the product is not delivering the "aha moment" before the trial ends.

**The Paid > Activated anomaly:**  
906 customers have subscriptions but only 404 have an activation event. This confirms the activation tracking gap noted in Section 3.3. These 502 paying-but-unactivated customers represent $105,539 in MRR and are the highest churn risk cohort in the business.

### 5.4 Acquisition Channel Analysis

| Source | Signups | Paid Conv | Churn Rate | ARPC | Efficiency Score |
|---|---|---|---|---|---|
| **ads** | 264 | 88.3% | **18.9%** | 279.70 | **71.6** - Best |
| outbound | 257 | 91.4% | 24.3% | 251.51 | 69.3 |
| organic | 248 | 90.3% | 26.8% | 234.91 | 66.1 |
| **referral** | 231 | **92.6%** | **29.0%** | 250.21 | 65.8 - Worst |

**Ads is the best channel** despite not having the highest raw conversion rate. It delivers the highest ARPC (279.70), the lowest churn (18.9%), and the highest efficiency score (71.6). These customers are better-fit and stay longer.

**Referral is the worst channel by quality.** It has the highest conversion rate (92.6%) but also the highest churn rate (29.0%) and the lowest efficiency score (65.8). Customers acquired through referral convert easily but leave fastest — suggesting the referral program may be attracting wrong-fit customers.

### 5.5 Segment & LTV Analysis

| Segment | Customers | ARPC | Churn Rate | Est. LTV | Avg Months Retained |
|---|---|---|---|---|---|
| **SMB** | 238 | 249.32 | 22.69% | **1,099** | 4.4 |
| Not Provided | 220 | 259.78 | 25.00% | 1,039 | 4.0 |
| Enterprise | 227 | 236.50 | 22.91% | 1,032 | 4.4 |
| **Mid-Market** | 221 | 272.60 | **28.05%** | **972** | 3.6 |

**Counter-intuitive finding: SMB has the highest LTV (1,099).**  
Although Enterprise has brand prestige, SMB customers churn at nearly the same rate (22.69% vs 22.91%) but their LTV is slightly higher because they represent a larger volume of consistent revenue. Mid-Market has the highest ARPC (272.60) but the worst churn rate (28.05%), resulting in the lowest LTV (972) and shortest retention (3.6 months average).

--- --------------------------------------------

## 6. Actionable Recommendations for Leadership

### Recommendation 1 — Fix Onboarding Before Spending More on Acquisition
**Problem:** 40.2% of trial users never reach activation. 502 paying customers never activated. 105,539 units of MRR is at immediate risk.  
**Action:** Audit the trial-to-activation journey. Identify the specific product action that correlates with long-term retention (the "aha moment"). Build an in-product onboarding checklist or guided tour that drives users to that action within the first 7 days of trial. Assign customer success outreach to the 68 customers with a risk score of 3 — they are paying, unactivated, and on the lowest price tier.  
**Expected impact:** Improving trial-to-activation from 59.8% to 70% would add ~ 68 activated customers per cohort, directly reducing the paid-but-unactivated MRR at risk of 105,539 units.

### Recommendation 2 — Reallocate Marketing Spend Toward Ads, Away from Referral
**Problem:** Referral converts well (92.6%) but churns fastest (29.0%) and has the lowest efficiency score (65.8). Ads converts slightly less (88.3%) but retains far better (81.1% retention) with the highest ARPC (279.70).  
**Action:** Reduce referral program incentives or tighten the qualification criteria for referral leads. Reallocate budget toward paid ads. If budget is fixed, even a 10% shift from referral to ads spend is expected to improve overall portfolio retention.  
**Evidence:** Ads customers generate 279.70 ARPC vs referral's 250.21 — a 12% higher revenue per customer — while churning 10 percentage points less frequently.

--- ---------------------------------------------

## 7. Power BI Dashboard

Built using Power BI Desktop connected directly to the `saasgtm` MySQL database. 
The dashboard is organized across 3 pages, each telling a distinct part of the business story.

### Page 1 — Acquisition & Funnel
Answers: *Where did growth come from and where is the funnel leaking?*

| Visual | Type | Insight |
|--------|------|---------|
| New MRR by Month | Line chart | Growth peaked March 2023, acquisition cliff visible from April |
| Customer Funnel | Funnel chart | 40.2% drop-off at Trial → Activation is the biggest leak |
| Signups by Month | Bar chart | January had the most signups (319), April near-zero |
| Signups by Acquisition Source | Pie chart | Ads (26.4%) and Outbound (25.7%) drive most signups |

*Note: Paid (906) exceeds Activated (404) in the funnel due to the activation event tracking gap documented in Section 3.3*

### Page 2 — Revenue & Churn
Answers: *How much revenue is at risk and who is most likely to churn?*

| Visual | Type | Insight |
|---|---|---|
| Active vs Canceled Customers | Bar chart | 683 active, 223 churned — churn is significant but stabilized |
| MRR at Risk (KPI) | Text card | 105,539 — 56% of active MRR from unactivated customers |
| Total MRR (KPI) | Text card | 187,242 current MRR |
| Churned Customers (KPI) | Text card | 223 total churned |
| Churn Risk by Level | Table | High risk (353 customers, 102,207 MRR) requires immediate CS action |

### Page 3 — Channel & Segment
Answers: *Which channels and segments are most valuable?*

| Visual | Type | Insight |
|---|---|---|
| Channel Efficiency Scorecard | Table | Ads best (efficiency 71.6), Referral worst (65.8) despite best conversion |
| Estimated LTV by Segment | Bar chart | SMB (1,099) > Enterprise (1,032) — counter-intuitive finding |
| ARPC by Segment | Bar chart | Mid-Market highest ARPC (272.60) |
| Churn Rate % by Segment | Bar chart | Mid-Market highest churn (28.05%) — explains low LTV despite high ARPC |

**Dashboard screenshots:** See `dashboard/` folder for all 3 pages.  

---

## 8. Limitations & What to Investigate Next

1. **Data ends in July 2023.** No acquisition data after April, no churn after July. Whether this is a business pause or a data pipeline issue is unknown and should be confirmed before any trend conclusions are presented to external stakeholders.

2. **Activation event tracking gap.** 55% of paying customers lack an activation event. Before acting on funnel metrics, the engineering team should confirm whether the activation event was always instrumented or only added partway through the observation period.

3. **No cost data.** LTV calculations use ARPC ÷ churn rate. True LTV should incorporate gross margin (revenue minus cost to serve). With cost data, the LTV rankings by segment may change.

4. **Mid-Market churn driver unknown.** Mid-Market has the worst churn rate (28.05%) despite the highest ARPC. A cohort analysis or customer interview study is needed to understand why Mid-Market customers leave faster than SMB despite paying more.

5. **Referral churn investigation.** Are referral customers churning because the product is wrong for them, or because the referral incentive attracts opportunistic signups? Segmenting referral churn by whether an incentive was used would clarify this.

---

## 9. How to Reproduce Results

```sql
-- 1. Create database
CREATE DATABASE saasgtm;
USE saasgtm;

-- 2. Run files in order:
--    01_raw_table_creation.sql  → creates raw_ staging tables, loads CSVs
--    02_data_cleaned.sql        → creates cleaned_ tables with type casting
--    03_core_metrics.sql        → MRR, ARR, ARPC, churn rates
--    04_funnel_analysis.sql     → funnel conversion and drop-off analysis
--    05_optional_analysis.sql   → channel efficiency, LTV, churn risk scoring
```

**Requirements:** MySQL 8.0+ (recursive CTEs require 8.0), MySQL Workbench or CLI  
**Data path:** Update file paths in `01_raw_table_creation.sql` before running

---

## 10. Assumptions Summary

| # | Assumption | Rationale |
|---|---|---|
| 1 | Missing segment → NULL (not 'Unknown') | NULL is SQL's native missing value marker; a string 'Unknown' pollutes GROUP BY results |
| 2 | Missing signup_date → excluded from time-series | Cannot fabricate a date; these 36 rows still appear in non-date queries |
| 3 | NULL end_date = active subscription | Standard SaaS data pattern; only canceled subscriptions have an end_date |
| 4 | Multi-subscription customers: keep all records | Date filters in MRR queries naturally allocate each subscription to correct months |
| 5 | Duplicate events: keep MIN(event_id) per group | Same customer + event_type + date cannot be legitimate; earliest ID = first log |
| 6 | Acquisition source = source from signup event | Signup event is the first touchpoint; subsequent events may reflect different sessions |
| 7 | Churn denominator = BOM customer count | Industry standard; prevents denominator inflation from same-month new customers |
| 8 | Churned = canceled subscription OR churned event (UNION) | Both signals are valid; UNION prevents double-counting |
| 9 | Paid > Activated count = tracking gap, not funnel error | 55% gap is too large to be behavioral; confirmed as instrumentation issue |
| 10 | LTV = ARPC ÷ monthly churn rate | Simplified LTV; gross margin unavailable in dataset |
