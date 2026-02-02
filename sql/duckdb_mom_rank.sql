CREATE OR REPLACE VIEW monthly_rank_changes
AS
WITH exploded AS (
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
    FROM exploded e
    JOIN transformed_data t ON e.domain = t.domain
)
SELECT
    domain,
    month,
    rank,
    prev_month_rank,
    (prev_month_rank - rank) AS rank_change,
    CASE
        WHEN prev_month_rank IS NULL OR prev_month_rank = 0 THEN NULL
        ELSE ROUND(((rank - prev_month_rank) / prev_month_rank) * 100, 2)
    END AS rank_change_percent
FROM (
    SELECT
        domain,
        month,
        rank,
        LAG(rank, 1) OVER (PARTITION BY domain ORDER BY month) AS prev_month_rank
    FROM monthly_ranks
) AS with_lag
ORDER BY domain, month;
