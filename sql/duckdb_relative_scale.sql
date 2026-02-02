CREATE OR REPLACE VIEW relative_ranking
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
),
visits_growth AS (
    SELECT DISTINCT
        domain,
        FIRST_VALUE(visits) OVER (PARTITION BY domain ORDER BY month) AS first_month_visits,
        FIRST_VALUE(visits) OVER (PARTITION BY domain ORDER BY month DESC) AS last_month_visits
    FROM monthly_visits
),
rank_exploded AS (
    SELECT
        domain,
        UNNEST(json_keys(rank_history)) AS month
    FROM transformed_data
),
monthly_ranks AS (
    SELECT
        e.domain,
        e.month,
        json_extract(t.rank_history, '$.' || e.month)::BIGINT AS rank
    FROM rank_exploded e
    JOIN transformed_data t ON e.domain = t.domain
),
rank_changes AS (
    SELECT DISTINCT
        domain,
        FIRST_VALUE(rank) OVER (PARTITION BY domain ORDER BY month) AS first_month_rank,
        FIRST_VALUE(rank) OVER (PARTITION BY domain ORDER BY month DESC) AS last_month_rank
    FROM monthly_ranks
),
metrics AS (
    SELECT
        v.domain,
        ROUND(((v.last_month_visits - v.first_month_visits) * 100.0 / NULLIF(v.first_month_visits, 0)), 2) AS visit_growth_pct,
        (r.first_month_rank - r.last_month_rank) AS rank_change
    FROM visits_growth v
    JOIN rank_changes r ON v.domain = r.domain
),
rankings AS (
    SELECT
        domain,
        visit_growth_pct,
        rank_change,
        RANK() OVER (ORDER BY visit_growth_pct DESC NULLS LAST) AS visits_rank,
        RANK() OVER (ORDER BY rank_change DESC NULLS LAST) AS rank_improvement_rank
    FROM metrics
)
SELECT
    domain,
    visit_growth_pct AS visit_growth,
    rank_change,
    visits_rank,
    rank_improvement_rank,
    (visits_rank + rank_improvement_rank) AS combined_rank
FROM rankings
ORDER BY combined_rank;
