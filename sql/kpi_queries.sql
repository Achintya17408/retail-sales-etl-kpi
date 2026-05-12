-- ============================================================
-- Project 2: Retail Sales — KPI SQL Queries
-- Target     : Accenture / Capgemini Data Analyst Interview
-- Dialect    : DuckDB / Snowflake (ANSI SQL + window functions)
-- Purpose    : Standalone SQL drills matching real interview
--              questions at consulting MNCs
-- ============================================================

-- ── 1. Running Revenue Total per City (Basic Window Function)
--      Interview question: "Calculate a running total of revenue
--      by city ordered by month."
-- ──────────────────────────────────────────────────────────
WITH monthly_city AS (
    SELECT
        city,
        ym,
        SUM(revenue) AS monthly_rev
    FROM transactions
    GROUP BY city, ym
)
SELECT
    city,
    ym,
    monthly_rev,
    SUM(monthly_rev) OVER (
        PARTITION BY city
        ORDER BY ym
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                   AS running_total,
    ROUND(
        SUM(monthly_rev) OVER (
            PARTITION BY city
            ORDER BY ym
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * 100.0 / SUM(monthly_rev) OVER (PARTITION BY city),
    2)                                                  AS cumulative_pct
FROM monthly_city
ORDER BY city, ym;


-- ── 2. Month-over-Month Revenue Growth per City
--      Interview: "Using LAG, find MoM revenue growth."
-- ──────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT city, ym, SUM(revenue) AS rev
    FROM transactions
    GROUP BY city, ym
)
SELECT
    city,
    ym,
    rev,
    LAG(rev) OVER (PARTITION BY city ORDER BY ym) AS prev_month_rev,
    ROUND(
        (rev - LAG(rev) OVER (PARTITION BY city ORDER BY ym))
        * 100.0 /
        NULLIF(LAG(rev) OVER (PARTITION BY city ORDER BY ym), 0),
    2)                                             AS mom_growth_pct
FROM monthly
ORDER BY city, ym;


-- ── 3. Customer Lifetime Value (LTV) with Percentile Ranking
--      Interview: "Rank customers by LTV within each city."
-- ──────────────────────────────────────────────────────────
WITH customer_ltv AS (
    SELECT
        customer_id,
        city,
        SUM(revenue)                    AS total_spend,
        COUNT(*)                        AS order_count,
        AVG(revenue)                    AS avg_order_value,
        MIN(txn_date)                   AS first_purchase,
        MAX(txn_date)                   AS last_purchase
    FROM transactions
    GROUP BY customer_id, city
)
SELECT
    customer_id,
    city,
    total_spend,
    order_count,
    ROUND(avg_order_value, 2) AS aov,
    RANK()  OVER (PARTITION BY city ORDER BY total_spend DESC) AS city_rank,
    NTILE(4) OVER (ORDER BY total_spend DESC)                  AS ltv_quartile,
    PERCENT_RANK() OVER (ORDER BY total_spend)                 AS percentile
FROM customer_ltv
ORDER BY city, city_rank;


-- ── 4. Category Contribution to Total Revenue (%)
--      Interview: "What % of total revenue does each
--      category contribute per city?"
-- ──────────────────────────────────────────────────────────
SELECT
    city,
    category,
    SUM(revenue)                                        AS cat_revenue,
    SUM(SUM(revenue)) OVER (PARTITION BY city)          AS city_total,
    ROUND(
        SUM(revenue) * 100.0 /
        SUM(SUM(revenue)) OVER (PARTITION BY city),
    2)                                                  AS revenue_share_pct,
    RANK() OVER (
        PARTITION BY city
        ORDER BY SUM(revenue) DESC
    )                                                   AS category_rank
FROM transactions
GROUP BY city, category
ORDER BY city, category_rank;


-- ── 5. Churned Customer Analysis
--      Interview: "Identify customers who haven't purchased
--      in 90 days and calculate their revenue impact."
-- ──────────────────────────────────────────────────────────
WITH last_purchase AS (
    SELECT
        customer_id,
        city,
        MAX(txn_date)  AS last_txn_date,
        SUM(revenue)   AS lifetime_revenue,
        COUNT(*)       AS total_orders
    FROM transactions
    GROUP BY customer_id, city
),
churn_status AS (
    SELECT
        *,
        DATEDIFF('day', last_txn_date,
                 (SELECT MAX(txn_date) FROM transactions)) AS days_inactive,
        CASE
            WHEN DATEDIFF('day', last_txn_date,
                          (SELECT MAX(txn_date) FROM transactions)) > 90
            THEN 'Churned'
            ELSE 'Active'
        END AS status
    FROM last_purchase
)
SELECT
    city,
    status,
    COUNT(*)                            AS customer_count,
    ROUND(AVG(lifetime_revenue), 2)     AS avg_ltv,
    ROUND(SUM(lifetime_revenue), 2)     AS total_revenue_at_risk,
    ROUND(AVG(total_orders), 1)         AS avg_orders,
    ROUND(AVG(days_inactive), 0)        AS avg_days_inactive
FROM churn_status
GROUP BY city, status
ORDER BY city, status;


-- ── 6. YoY Revenue Growth (Self-Join Pattern)
--      Interview: "Without window functions, write a query
--      to compute year-on-year growth."
-- ──────────────────────────────────────────────────────────
WITH yearly AS (
    SELECT
        EXTRACT(YEAR FROM txn_date) AS yr,
        city,
        category,
        SUM(revenue) AS annual_rev
    FROM transactions
    GROUP BY yr, city, category
)
SELECT
    t1.yr          AS current_year,
    t1.city,
    t1.category,
    t1.annual_rev  AS current_rev,
    t0.annual_rev  AS prior_year_rev,
    ROUND((t1.annual_rev - t0.annual_rev) * 100.0 /
          NULLIF(t0.annual_rev, 0), 2) AS yoy_growth_pct
FROM yearly      t1
LEFT JOIN yearly t0
    ON  t0.yr       = t1.yr - 1
    AND t0.city     = t1.city
    AND t0.category = t1.category
WHERE t1.yr > (SELECT MIN(EXTRACT(YEAR FROM txn_date)) FROM transactions)
ORDER BY t1.city, t1.category, t1.yr;


-- ── 7. Cohort Retention Analysis
--      Interview: "Build a customer cohort table showing
--      how many customers return in months 1, 2, 3 after
--      their first purchase."
-- ──────────────────────────────────────────────────────────
WITH first_purchase AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(txn_date)) AS cohort_month
    FROM transactions
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT
        t.customer_id,
        fp.cohort_month,
        DATE_TRUNC('month', t.txn_date)  AS activity_month,
        DATEDIFF('month', fp.cohort_month,
                 DATE_TRUNC('month', t.txn_date)) AS months_after_first
    FROM transactions t
    JOIN first_purchase fp USING (customer_id)
)
SELECT
    cohort_month,
    months_after_first,
    COUNT(DISTINCT customer_id)                    AS returning_customers,
    MAX(COUNT(DISTINCT customer_id)) OVER (
        PARTITION BY cohort_month
    )                                              AS cohort_size,
    ROUND(
        COUNT(DISTINCT customer_id) * 100.0 /
        MAX(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month
        ), 1
    )                                              AS retention_pct
FROM monthly_activity
WHERE months_after_first BETWEEN 0 AND 6
GROUP BY cohort_month, months_after_first
ORDER BY cohort_month, months_after_first;
