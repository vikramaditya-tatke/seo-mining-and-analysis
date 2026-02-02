CREATE OR REPLACE VIEW monthly_visit_changes
AS
WITH exploded AS (
    SELECT
        domain,
        UNNEST(json_keys(visits_history)) AS month
    FROM transformed_data
),
monthly_visits AS (
    SELECT
        e.domain,
        e.month,
        json_extract(t.visits_history, '$.' || e.month)::BIGINT AS visits
    FROM exploded e
    JOIN transformed_data t ON e.domain = t.domain
)
SELECT
    domain,
    month,
    visits,
    prev_month_visits,
    CASE
        WHEN prev_month_visits IS NULL OR prev_month_visits = 0 THEN NULL
        ELSE ROUND(((visits - prev_month_visits) / prev_month_visits) * 100, 2)
    END AS mom_growth_percent
FROM (
    SELECT
        domain,
        month,
        visits,
        LAG(visits, 1) OVER (PARTITION BY domain ORDER BY month) AS prev_month_visits
    FROM monthly_visits
) AS with_lag
ORDER BY domain, month;
