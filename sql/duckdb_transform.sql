CREATE OR REPLACE VIEW transformed_data
AS
SELECT
    domain,
    global_rank,
    total_visits,
    pages_per_visit,
    last_month_change,
    bounce_rate_raw,
    avg_visit_duration_raw,
    visits_history_raw AS visits_history,

    MAP_FROM_ENTRIES(
        LIST_TRANSFORM(
            top_countries_raw,
            x -> struct_pack(
                k := x.countryAlpha2Code,
                v := ROUND(x.visitsShare * 100, 2)
            )
        )
    )::JSON AS top_countries,

    MAP_FROM_ENTRIES(
        LIST_TRANSFORM(
            age_distribution_raw,
            x -> struct_pack(
                k := CONCAT(
                    CAST(x.minAge AS VARCHAR),
                    '-',
                    COALESCE(CAST(x.maxAge AS VARCHAR), '65+')
                ),
                v := ROUND(x.value * 100, 2)
            )
        )
    )::JSON AS age_distribution,

    MAP_FROM_ENTRIES(
        LIST_TRANSFORM(
            from_json(
                json_extract(rank_history_raw, '$."' || domain || '"'),
                '[{"date": "TIMESTAMPTZ", "rank": "BIGINT"}]'
            ),
            x -> STRUCT_PACK(k := x."date"::VARCHAR, v := x."rank")
        )
    )::JSON AS rank_history
FROM
    source_data
