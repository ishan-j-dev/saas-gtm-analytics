"""
06_python_exploration.py
========================

Purpose:
  - Load raw CSV files using pandas
  - Initial data exploration (shape, dtypes, distributions)
  - Validation and sanity checks (nulls, duplicates, referential integrity,
    date formats, business logic)
  - Load cleaned data into MySQL via mysql-connector-python

Libraries: pandas, numpy, mysql-connector-python
"""

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import warnings
warnings.filterwarnings("ignore")

# =============================================================================
# CONFIG
# =============================================================================

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",         
    "password": "aNj@1234567",
    "database": "saasgtm"
}

CSV_PATHS = {
    "customers":     "customers.csv",
    "subscriptions": "subscriptions.csv",
    "events":        "events.csv"
}

# =============================================================================
# SECTION 1 — LOAD CSV FILES
# =============================================================================

print("=" * 65)
print("SECTION 1: LOADING CSV FILES")
print("=" * 65)

customers     = pd.read_csv(CSV_PATHS["customers"])
subscriptions = pd.read_csv(CSV_PATHS["subscriptions"])
events        = pd.read_csv(CSV_PATHS["events"])

print(f"customers     loaded: {customers.shape[0]:>5} rows  x  {customers.shape[1]} cols")
print(f"subscriptions loaded: {subscriptions.shape[0]:>5} rows  x  {subscriptions.shape[1]} cols")
print(f"events        loaded: {events.shape[0]:>5} rows  x  {events.shape[1]} cols")

# =============================================================================
# SECTION 2 — INITIAL DATA EXPLORATION
# =============================================================================

print("\n" + "=" * 65)
print("SECTION 2: DATA EXPLORATION")
print("=" * 65)

# ── 2.1 Schema overview ──────────────────────────────────────────
print("\n── customers dtypes ──")
print(customers.dtypes.to_string())

print("\n── subscriptions dtypes ──")
print(subscriptions.dtypes.to_string())

print("\n── events dtypes ──")
print(events.dtypes.to_string())

# ── 2.2 Sample rows ──────────────────────────────────────────────
print("\n── customers sample (head 3) ──")
print(customers.head(3).to_string(index=False))

print("\n── subscriptions sample (head 3) ──")
print(subscriptions.head(3).to_string(index=False))

print("\n── events sample (head 3) ──")
print(events.head(3).to_string(index=False))

# ── 2.3 Categorical distributions ────────────────────────────────
print("\n── customers: segment distribution ──")
print(customers["segment"].value_counts(dropna=False).to_string())

print("\n── customers: country distribution ──")
print(customers["country"].value_counts(dropna=False).to_string())

print("\n── subscriptions: status distribution ──")
print(subscriptions["status"].value_counts(dropna=False).to_string())

print("\n── subscriptions: monthly_price distribution ──")
print(subscriptions["monthly_price"].value_counts().sort_index().to_string())

print("\n── events: event_type distribution ──")
print(events["event_type"].value_counts(dropna=False).to_string())

print("\n── events: source distribution ──")
print(events["source"].value_counts(dropna=False).to_string())

# ── 2.4 Numeric summary ───────────────────────────────────────────
print("\n── subscriptions: monthly_price summary stats ──")
print(subscriptions["monthly_price"].describe().to_string())

# ── 2.5 Date ranges ───────────────────────────────────────────────
# Parse dates (coerce errors to NaT so we can inspect them)
customers["signup_date_parsed"]          = pd.to_datetime(customers["signup_date"], errors="coerce")
subscriptions["start_date_parsed"]       = pd.to_datetime(subscriptions["start_date"], errors="coerce")
subscriptions["end_date_parsed"]         = pd.to_datetime(subscriptions["end_date"], errors="coerce")
events["event_date_parsed"]              = pd.to_datetime(events["event_date"], errors="coerce")

print("\n── date ranges ──")
print(f"customers    signup_date : {customers['signup_date_parsed'].min().date()} → {customers['signup_date_parsed'].max().date()}")
print(f"subscriptions start_date : {subscriptions['start_date_parsed'].min().date()} → {subscriptions['start_date_parsed'].max().date()}")
print(f"subscriptions end_date   : {subscriptions['end_date_parsed'].min().date()} → {subscriptions['end_date_parsed'].max().date()}  (nulls = open/active)")
print(f"events        event_date : {events['event_date_parsed'].min().date()} → {events['event_date_parsed'].max().date()}")

# =============================================================================
# SECTION 3 — VALIDATION & SANITY CHECKS
# =============================================================================

print("\n" + "=" * 65)
print("SECTION 3: VALIDATION & SANITY CHECKS")
print("=" * 65)

issues_found = 0

def flag(label, condition, detail=""):
    global issues_found
    status = "⚠  ISSUE" if condition else "✓  OK"
    issues_found += condition
    print(f"  {status}  {label}" + (f"  →  {detail}" if detail else ""))

# ── 3.1 Null checks ──────────────────────────────────────────────
print("\n[3.1] Null / missing value counts")

# Only check original columns, not the _parsed helper columns we added
orig_cust_cols = ["customer_id", "signup_date", "segment", "country", "is_enterprise"]
orig_sub_cols  = ["subscription_id", "customer_id", "start_date", "end_date", "monthly_price", "status"]
orig_evt_cols  = ["event_id", "customer_id", "event_type", "event_date", "source"]

c_nulls = customers[orig_cust_cols].isnull().sum()
s_nulls = subscriptions[orig_sub_cols].isnull().sum()
e_nulls = events[orig_evt_cols].isnull().sum()

print("\n  customers:")
for col, n in c_nulls.items():
    flag(f"customers.{col} nulls", n > 0, f"{n} nulls")

print("\n  subscriptions:")
for col, n in s_nulls.items():
    # end_date nulls are expected (= still active), not a data quality issue
    if col == "end_date":
        print(f"  ✓  OK   subscriptions.end_date nulls  →  {n} nulls (expected: NULL = active subscription)")
    else:
        flag(f"subscriptions.{col} nulls", n > 0, f"{n} nulls")

print("\n  events:")
for col, n in e_nulls.items():
    flag(f"events.{col} nulls", n > 0, f"{n} nulls")

# ── 3.2 Duplicate primary keys ───────────────────────────────────
print("\n[3.2] Duplicate primary key checks")
cust_dup  = customers["customer_id"].duplicated().sum()
sub_dup   = subscriptions["subscription_id"].duplicated().sum()
evt_dup   = events["event_id"].duplicated().sum()

flag("customers.customer_id duplicates",     cust_dup > 0, f"{cust_dup} dupes")
flag("subscriptions.subscription_id dupes",  sub_dup > 0,  f"{sub_dup} dupes")
flag("events.event_id duplicates",           evt_dup > 0,  f"{evt_dup} dupes")

# ── 3.3 Duplicate events (logical) ───────────────────────────────
print("\n[3.3] Logical event duplicates (same customer + type + date)")
evt_logical_dup = events.duplicated(subset=["customer_id", "event_type", "event_date"]).sum()
flag("Logical event duplicates", evt_logical_dup > 0, f"{evt_logical_dup} duplicates — dedup needed before analysis")

# ── 3.4 Referential integrity ────────────────────────────────────
print("\n[3.4] Referential integrity")
valid_cust_ids = set(customers["customer_id"])

subs_orphan  = subscriptions[~subscriptions["customer_id"].isin(valid_cust_ids)]
evt_orphan   = events[~events["customer_id"].isin(valid_cust_ids)]

flag("subscriptions: customer_id in customers", len(subs_orphan) > 0,
     f"{len(subs_orphan)} subscriptions reference unknown customer_ids")
flag("events: customer_id in customers", len(evt_orphan) > 0,
     f"{len(evt_orphan)} events reference unknown customer_ids")

# ── 3.5 Date format / parse errors ───────────────────────────────
print("\n[3.5] Date parse errors (non-standard formats)")
bad_signup = customers["signup_date_parsed"].isna() & customers["signup_date"].notna()
bad_start  = subscriptions["start_date_parsed"].isna() & subscriptions["start_date"].notna()
bad_end    = subscriptions["end_date_parsed"].isna() & subscriptions["end_date"].notna()
bad_event  = events["event_date_parsed"].isna() & events["event_date"].notna()

flag("customers.signup_date parse errors",       bad_signup.sum() > 0, f"{bad_signup.sum()} rows")
flag("subscriptions.start_date parse errors",    bad_start.sum() > 0,  f"{bad_start.sum()} rows")
flag("subscriptions.end_date parse errors",      bad_end.sum() > 0,    f"{bad_end.sum()} rows")
flag("events.event_date parse errors",           bad_event.sum() > 0,  f"{bad_event.sum()} rows")

# customers with NULL signup_date (separate from parse errors)
null_signup = customers["signup_date"].isna().sum()
flag("customers.signup_date null (missing entirely)", null_signup > 0,
     f"{null_signup} customers have no signup_date")

# ── 3.6 Business logic: end_date after start_date ────────────────
print("\n[3.6] Business logic checks")
subs_with_end = subscriptions[subscriptions["end_date_parsed"].notna()].copy()
end_before_start = (subs_with_end["end_date_parsed"] < subs_with_end["start_date_parsed"]).sum()
flag("end_date before start_date", end_before_start > 0, f"{end_before_start} records")

# canceled subs should all have an end_date
canceled_no_end = subscriptions[
    (subscriptions["status"] == "canceled") &
    (subscriptions["end_date"].isna())
]
flag("canceled status but no end_date", len(canceled_no_end) > 0,
     f"{len(canceled_no_end)} records")

# active subs should NOT have an end_date
active_with_end = subscriptions[
    (subscriptions["status"] == "active") &
    (subscriptions["end_date"].notna())
]
flag("active status but has end_date", len(active_with_end) > 0,
     f"{len(active_with_end)} records")

# ── 3.7 Price tier validation ─────────────────────────────────────
print("\n[3.7] Price tier validation")
valid_tiers = {49, 79, 99, 149, 299, 499, 699}
invalid_prices = subscriptions[~subscriptions["monthly_price"].isin(valid_tiers)]
flag("unexpected monthly_price values", len(invalid_prices) > 0,
     f"{len(invalid_prices)} records with prices outside expected tiers: {sorted(subscriptions['monthly_price'].unique())}")

# ── 3.8 Segment / country / event_type allowed values ─────────────
print("\n[3.8] Allowed value checks")
valid_segments   = {"SMB", "Mid-Market", "Enterprise"}
valid_countries  = {"US", "UK", "AU", "CA", "DE", "IN"}
valid_statuses   = {"active", "canceled"}
valid_event_types = {"signup", "trial_start", "activated", "churned"}
valid_sources    = {"organic", "ads", "referral", "outbound"}

bad_seg   = customers[customers["segment"].notna() & ~customers["segment"].isin(valid_segments)]
bad_cntry = customers[~customers["country"].isin(valid_countries)]
bad_stat  = subscriptions[~subscriptions["status"].isin(valid_statuses)]
bad_evt   = events[~events["event_type"].isin(valid_event_types)]
bad_src   = events[~events["source"].isin(valid_sources)]

flag("unexpected segment values",    len(bad_seg) > 0,   f"{len(bad_seg)} rows: {bad_seg['segment'].unique().tolist()}")
flag("unexpected country values",    len(bad_cntry) > 0, f"{len(bad_cntry)} rows")
flag("unexpected status values",     len(bad_stat) > 0,  f"{len(bad_stat)} rows")
flag("unexpected event_type values", len(bad_evt) > 0,   f"{len(bad_evt)} rows")
flag("unexpected source values",     len(bad_src) > 0,   f"{len(bad_src)} rows")

# ── 3.9 Funnel sequence sanity check ─────────────────────────────
print("\n[3.9] Funnel coverage checks")
signed_up   = set(events[events["event_type"] == "signup"]["customer_id"])
trialled    = set(events[events["event_type"] == "trial_start"]["customer_id"])
activated   = set(events[events["event_type"] == "activated"]["customer_id"])
paid        = set(subscriptions["customer_id"])
churned_evt = set(events[events["event_type"] == "churned"]["customer_id"])

print(f"  Signed up:          {len(signed_up):>5}")
print(f"  Started trial:      {len(trialled):>5}  ({len(trialled)/len(signed_up)*100:.1f}% of signups)")
print(f"  Activated:          {len(activated):>5}  ({len(activated)/len(trialled)*100:.1f}% of trials)")
print(f"  Paid:               {len(paid):>5}  ({len(paid)/len(signed_up)*100:.1f}% of signups)")
print(f"  Churned (event):    {len(churned_evt):>5}")

paid_no_activation = paid - activated
print(f"\n  Paid but no activation event: {len(paid_no_activation)} customers ({len(paid_no_activation)/len(paid)*100:.1f}% of paid)")
print(f"  → These customers are paying but never hit the activation milestone.")
print(f"    Likely a tracking gap or onboarding failure. Flagged as MRR at risk.")

# ── 3.10 Multi-subscription customers ─────────────────────────────
print("\n[3.10] Multi-subscription customers")
sub_counts = subscriptions.groupby("customer_id")["subscription_id"].count()
multi_sub  = sub_counts[sub_counts > 1]
print(f"  Customers with >1 subscription: {len(multi_sub)}")
if len(multi_sub) > 0:
    print(f"  Max subscriptions per customer: {multi_sub.max()}")
    print(f"  → Likely upgrades/downgrades. All rows retained; date filters handle overlap in SQL.")

# ── Validation summary ────────────────────────────────────────────
print(f"\n{'=' * 65}")
print(f"VALIDATION SUMMARY: {issues_found} issue(s) flagged")
print(f"{'=' * 65}")

# =============================================================================
# SECTION 4 — LOAD TO MYSQL
# =============================================================================

print("\n" + "=" * 65)
print("SECTION 4: LOADING CLEANED DATA TO MYSQL")
print("=" * 65)

def get_connection():
    """Return a MySQL connection using DB_CONFIG."""
    return mysql.connector.connect(**DB_CONFIG)

def load_table(conn, table_name, df, expected_min_rows):
    """Truncate raw table and bulk-insert rows from a DataFrame."""
    cursor = conn.cursor()

    # Count existing rows first
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    existing = cursor.fetchone()[0]
    print(f"\n  [{table_name}] existing rows before load: {existing}")

    # Confirm expected minimum is met in CSV
    if len(df) < expected_min_rows:
        print(f"  ⚠  ABORT: CSV has only {len(df)} rows, expected >= {expected_min_rows}. Skipping load.")
        cursor.close()
        return

    # Build INSERT statement dynamically from DataFrame columns
    cols        = ", ".join(df.columns.tolist())
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql  = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})"

    # Replace NaN with None so MySQL treats them as NULL
    rows = [
        tuple(None if (v is not None and not isinstance(v, str) and np.isnan(v)) else v
              for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    cursor.executemany(insert_sql, rows)
    conn.commit()

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    new_count = cursor.fetchone()[0]
    print(f"  [{table_name}] rows after load: {new_count}  (+{new_count - existing} inserted)")
    cursor.close()

# Prepare DataFrames for loading (raw tables only — column names must match)
# Drop the parsed helper columns added during exploration
customers_load = customers[["customer_id", "signup_date", "segment", "country", "is_enterprise"]]
subscriptions_load = subscriptions[["subscription_id", "customer_id", "start_date", "end_date", "monthly_price", "status"]]
events_load = events[["event_id", "customer_id", "event_type", "event_date", "source"]]

print("\nAttempting MySQL connection...")
try:
    conn = get_connection()
    if conn.is_connected():
        print(f"  Connected to MySQL: {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['database']}")

        load_table(conn, "raw_customers",     customers_load,     1000)
        load_table(conn, "raw_subscriptions", subscriptions_load, 941)
        load_table(conn, "raw_events",        events_load,        2411)

        conn.close()
        print("\n  Connection closed.")
    else:
        print("  ⚠  Connection object returned but is_connected() = False.")

except Error as e:
    print(f"\n  ⚠  MySQL connection failed: {e}")
    print(  "     Check DB_CONFIG credentials and ensure MySQL server is running.")
    print(  "     All Python exploration and validation above completed successfully.")
    print(  "     SQL files (01–05) were run directly in MySQL Workbench.")

# =============================================================================
# SECTION 5 — QUICK CROSS-CHECK AGAINST MYSQL RESULTS
# =============================================================================

print("\n" + "=" * 65)
print("SECTION 5: CROSS-CHECKS — PYTHON vs SQL RESULTS")
print("=" * 65)
print("  Validating Python-computed metrics against known SQL outputs.\n")

# Recompute key metrics from raw CSV and compare to SQL results
# SQL results from 03_core_metrics.sql / 05_optional_analysis.sql

sql_known = {
    "active_customers":  683,
    "total_paid":        906,
    "total_trials":      676,
    "total_activated":   404,
    "total_churned":     223,
}

# Active = has subscription with status = 'active'
py_active    = subscriptions[subscriptions["status"] == "active"]["customer_id"].nunique()
py_paid      = subscriptions["customer_id"].nunique()
py_trials    = events[events["event_type"] == "trial_start"]["customer_id"].nunique()
py_activated = events[events["event_type"] == "activated"]["customer_id"].nunique()

# Churned = canceled subscription OR churned event
canceled_ids = set(subscriptions[subscriptions["status"] == "canceled"]["customer_id"])
churned_ids  = set(events[events["event_type"] == "churned"]["customer_id"])
py_churned   = len(canceled_ids | churned_ids)

py_results = {
    "active_customers": py_active,
    "total_paid":       py_paid,
    "total_trials":     py_trials,
    "total_activated":  py_activated,
    "total_churned":    py_churned,
}

print(f"  {'Metric':<25} {'Python':>8}  {'SQL':>8}  {'Match':>6}")
print(f"  {'-'*55}")
for metric, sql_val in sql_known.items():
    py_val = py_results[metric]
    match  = "✓" if py_val == sql_val else "⚠ DIFF"
    print(f"  {metric:<25} {py_val:>8}  {sql_val:>8}  {match:>6}")

# MRR cross-check (Python)
active_subs = subscriptions[subscriptions["status"] == "active"]
py_mrr = active_subs["monthly_price"].sum()
sql_mrr = 187242
mrr_match = "✓" if py_mrr == sql_mrr else "⚠ DIFF"
print(f"  {'current_mrr':<25} {py_mrr:>8}  {sql_mrr:>8}  {mrr_match:>6}")

print(f"\n  Note: 'total_churned' difference is an intentional finding:")
print(f"    SQL counted only canceled subscriptions (223).")
print(f"    Python counted canceled subs UNION churned events ({py_churned} unique customers).")
print(f"    237 customers have a 'churned' event; some overlap with canceled subscriptions.")
print(f"    Recommendation: align the SQL churn definition to also capture churned events.")
print(f"  Other minor differences can occur due to multi-subscription date logic.")

print("\n" + "=" * 65)
print("SCRIPT COMPLETE")
print("=" * 65)
