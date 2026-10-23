CREATE OR REPLACE TABLE source_data
AS
SELECT
    domain,
    global_rank,
    total_visits,
    pages_per_visit,
    last_month_change,
    bounce_rate_raw,
    avg_visit_duration_raw,
    bounce_rate_percent,
    avg_visit_duration_seconds,
    visits_history_raw::JSON AS visits_history_raw,
    from_json(top_countries_raw, '[{"countryAlpha2Code": "VARCHAR", "visitsShare": "DOUBLE"}]') AS top_countries_raw,
    from_json(age_distribution_raw, '[{"minAge": "INT", "maxAge": "INT", "value": "DOUBLE"}]') AS age_distribution_raw,
    rank_history_raw::JSON AS rank_history_raw
FROM read_csv('{csv_path}', types = {
    'domain': 'VARCHAR',
    'global_rank': 'BIGINT',
    'total_visits': 'BIGINT',
    'pages_per_visit': 'DOUBLE',
    'last_month_change': 'DOUBLE',
    'bounce_rate_raw': 'VARCHAR',
    'avg_visit_duration_raw': 'VARCHAR',
    'bounce_rate_percent': 'DOUBLE',
    'avg_visit_duration_seconds': 'BIGINT'
});
