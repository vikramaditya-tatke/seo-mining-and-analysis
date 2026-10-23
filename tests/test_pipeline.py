"""Exercise the real HTML -> CSV -> DuckDB -> analysis -> PNG flow offline."""

import json
import struct
import subprocess
import sys
from pathlib import Path

import duckdb
import polars as pl
import pytest

from src.analysis import create_views
from src.etl_pipeline import (
    extract,
    load_extraction_config,
    pipeline,
    stage_1_polars_transform,
)

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/scraped_html"
SQL = ROOT / "sql"
CHARTS = {
    "mom_visits_startups.png",
    "mom_visits_google.png",
    "mom_rank_startups.png",
    "mom_rank_google.png",
    "mom_rank_byte_trading.png",
    "relative_ranking.png",
}
# Independently recorded values from the five supplied October-December scrapes.
EXPECTED = {
    "byte-trading.com": {
        "ranks": [5467516, 9628760, 7277936],
        "visits": [None, None, None],
        "duration": None,
        "bounce": None,
        "total": 0,
    },
    "crunchbase.com": {
        "ranks": [5721, 6054, 7435],
        "visits": [10489281, 10549539, 9111589],
        "duration": 146,
        "bounce": 55.43,
        "total": 9111589,
    },
    "google.com": {
        "ranks": [1, 1, 1],
        "visits": [87023594527, 85085372857, 86429287970],
        "duration": 635,
        "bounce": 28.77,
        "total": 86429287970,
    },
    "pitchbook.com": {
        "ranks": [15181, 14675, 18054],
        "visits": [2840358, 2974914, 2474684],
        "duration": 248,
        "bounce": 36.03,
        "total": 2474684,
    },
    "stripe.com": {
        "ranks": [1131, 1080, 1062],
        "visits": [43224435, 45589745, 46952509],
        "duration": 224,
        "bounce": 53.17,
        "total": 46952509,
    },
}


def run_cli(output_root, *extra):
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "main.py"),
            "--output-root",
            str(output_root),
            *extra,
        ],
        cwd=output_root.parent,
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.fixture(scope="module")
def completed_run(tmp_path_factory):
    # Also covers a new database parent directory and SQL quoting of file paths.
    output = tmp_path_factory.mktemp("e2e") / "run's outputs"
    result = run_cli(output)
    assert result.returncode == 0, result.stdout + result.stderr
    return output


def test_extraction_and_typed_database(completed_run):
    csv = pl.read_csv(completed_run / "data/transformed/polars_dump.csv")
    assert csv.height == 5
    assert set(csv["domain"]) == set(EXPECTED)
    with duckdb.connect(str(completed_run / "db/scraped_data.duckdb")) as conn:
        assert conn.execute("SELECT count(*) FROM source_data").fetchone() == (5,)
        types = {
            row[0]: row[1]
            for row in conn.execute("DESCRIBE transformed_data").fetchall()
        }
        assert types["global_rank"] == types["total_visits"] == "BIGINT"
        assert types["pages_per_visit"] == types["last_month_change"] == "DOUBLE"
        assert types["bounce_rate_percent"] == "DOUBLE"
        assert types["avg_visit_duration_seconds"] == "BIGINT"
        for name in [
            "top_countries",
            "age_distribution",
            "rank_history",
            "visits_history",
        ]:
            assert types[name] == "JSON"
        rows = conn.execute(
            """SELECT domain, global_rank, total_visits, bounce_rate_percent,
                      avg_visit_duration_seconds
               FROM transformed_data ORDER BY domain"""
        ).fetchall()
        assert len(rows) == 5
        for domain, rank, total, bounce, duration in rows:
            expected = EXPECTED[domain]
            assert (rank, total, bounce, duration) == (
                expected["ranks"][-1],
                expected["total"],
                expected["bounce"],
                expected["duration"],
            )
            csv_row = csv.filter(pl.col("domain") == domain).row(0, named=True)
            assert csv_row["avg_visit_duration_seconds"] == duration
            assert csv_row["bounce_rate_percent"] == bounce
        countries, ages = conn.execute(
            "SELECT top_countries, age_distribution FROM transformed_data "
            "WHERE domain='google.com'"
        ).fetchone()
        assert json.loads(countries) == {
            "US": 27.04,
            "IN": 4.51,
            "BR": 4.39,
            "GB": 3.81,
            "JP": 3.70,
        }
        assert json.loads(ages) == {
            "25-34": 30.32,
            "18-24": 23.86,
            "35-44": 18.79,
            "45-54": 12.75,
            "55-64": 8.63,
            "65+": 5.64,
        }


def test_monthly_analyses_and_relative_ranking(completed_run):
    with duckdb.connect(str(completed_run / "db/scraped_data.duckdb")) as conn:
        assert conn.execute("SELECT count(*) FROM monthly_rank_changes").fetchone() == (
            15,
        )
        assert conn.execute(
            "SELECT count(visits) FROM monthly_visit_changes"
        ).fetchone() == (12,)
        for domain, expected in EXPECTED.items():
            ranks = conn.execute(
                """SELECT month, rank, prev_month_rank, rank_change, rank_change_percent
                   FROM monthly_rank_changes WHERE domain=? ORDER BY month""",
                [domain],
            ).fetchall()
            visits = conn.execute(
                """SELECT month, visits, prev_month_visits, mom_growth_percent
                   FROM monthly_visit_changes WHERE domain=? ORDER BY month""",
                [domain],
            ).fetchall()
            assert [row[0][:10] for row in ranks] == [
                "2022-10-01",
                "2022-11-01",
                "2022-12-01",
            ]
            assert [row[0][:10] for row in visits] == [
                "2022-10-01",
                "2022-11-01",
                "2022-12-01",
            ]
            for index, (_, rank, previous, change, percent) in enumerate(ranks):
                assert rank == expected["ranks"][index]
                prior = expected["ranks"][index - 1] if index else None
                assert previous == prior
                assert change == (prior - rank if prior else None)
                assert percent == (
                    round((rank / prior - 1) * 100, 2) if prior else None
                )
            for index, (_, value, previous, growth) in enumerate(visits):
                assert value == expected["visits"][index]
                prior = expected["visits"][index - 1] if index else None
                assert previous == prior
                assert growth == (
                    round((value / prior - 1) * 100, 2) if prior else None
                )
        assert conn.execute(
            "SELECT * FROM relative_ranking ORDER BY combined_rank, domain"
        ).fetchall() == [
            ("stripe.com", 8.62, 69, 1, 1, 2),
            ("google.com", -0.68, 0, 2, 2, 4),
            ("crunchbase.com", -13.13, -1714, 4, 3, 7),
            ("pitchbook.com", -12.87, -2873, 3, 4, 7),
            ("byte-trading.com", None, -1810420, 5, 5, 10),
        ]


def test_all_six_pngs_are_generated(completed_run):
    charts = completed_run / "data/visualizations"
    assert {path.name for path in charts.glob("*.png")} == CHARTS
    for name in CHARTS:
        content = (charts / name).read_bytes()
        assert content.startswith(b"\x89PNG\r\n\x1a\n")
        assert content.endswith(b"\x00\x00\x00\x00IEND\xaeB`\x82")
        width, height = struct.unpack(">II", content[16:24])
        assert width >= 1800 and height >= 1500
        assert len(content) > 10000


def test_rerun_replaces_data_without_duplicates(completed_run):
    result = run_cli(completed_run)
    assert result.returncode == 0, result.stdout + result.stderr
    with duckdb.connect(str(completed_run / "db/scraped_data.duckdb")) as conn:
        for table, count in [
            ("source_data", 5),
            ("transformed_data", 5),
            ("monthly_rank_changes", 15),
            ("relative_ranking", 5),
        ]:
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (count,)
        assert conn.execute(
            "SELECT avg_visit_duration_seconds FROM transformed_data "
            "WHERE domain='google.com'"
        ).fetchone() == (635,)


@pytest.mark.parametrize(
    "contents",
    [
        "<html>No application data</html>",
        "<script>window.__APP_DATA__ = {bad};</script>",
        "<script>window.__APP_DATA__ = null;</script>",
    ],
)
def test_extract_preserves_none_on_failure_contract(tmp_path, contents):
    html = tmp_path / "similarweb-invalid.html"
    html.write_text(contents)
    assert extract(html) is None


def test_duration_does_not_overflow_for_hours(tmp_path):
    data = extract(RAW / "similarweb-google-com.html")
    data["layout"]["data"]["overview"]["visitsAvgDurationFormatted"] = "01:10:35"
    csv_path = tmp_path / "data.csv"
    stage_1_polars_transform(
        [data], load_extraction_config(ROOT / "config/schema_config.json"), csv_path
    )
    assert pl.read_csv(csv_path)["avg_visit_duration_seconds"].to_list() == [4235]


@pytest.mark.parametrize("invalid_file", [False, True])
def test_cli_preserves_empty_input_noop_and_reports_processing_failure(
    tmp_path, invalid_file
):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    if invalid_file:
        (inputs / "similarweb-broken.html").write_text("<html>Blocked page</html>")
    output = tmp_path / "output"
    result = run_cli(output, "--input-dir", str(inputs))
    if invalid_file:
        assert result.returncode != 0
        assert "Extraction failed" in result.stderr
    else:
        assert result.returncode == 0
        assert "No HTML files" in result.stderr
    assert not (output / "db/scraped_data.duckdb").exists()


def test_failed_extraction_does_not_publish_a_null_domain_row(tmp_path):
    invalid_html = tmp_path / "similarweb-blocked.html"
    invalid_html.write_text("<html>Blocked page</html>")
    with pytest.raises(ValueError, match="Extraction failed") as error:
        pipeline(
            [RAW / "similarweb-google-com.html", invalid_html],
            ROOT / "config/schema_config.json",
            tmp_path,
            tmp_path / "data.duckdb",
            SQL / "duckdb_load.sql",
            SQL / "duckdb_transform.sql",
        )
    assert str(invalid_html) in str(error.value)
    assert not (tmp_path / "polars_dump.csv").exists()
    assert not (tmp_path / "data.duckdb").exists()


def test_sparse_dataset_keeps_domain_and_numeric_null_types(tmp_path):
    db = tmp_path / "db/data.duckdb"
    pipeline(
        [RAW / "similarweb-byte-trading-com.html"],
        ROOT / "config/schema_config.json",
        tmp_path / "csv",
        db,
        SQL / "duckdb_load.sql",
        SQL / "duckdb_transform.sql",
    )
    create_views(
        db,
        SQL / "duckdb_mom_visits.sql",
        SQL / "duckdb_mom_rank.sql",
        SQL / "duckdb_relative_scale.sql",
    )
    with duckdb.connect(str(db)) as conn:
        assert conn.execute("SELECT * FROM relative_ranking").fetchall() == [
            ("byte-trading.com", None, -1810420, 1, 1, 2)
        ]
        assert conn.execute(
            """SELECT bounce_rate_percent, avg_visit_duration_seconds,
                      typeof(bounce_rate_percent), typeof(avg_visit_duration_seconds)
               FROM transformed_data"""
        ).fetchone() == (None, None, "DOUBLE", "BIGINT")


def test_zero_baselines_do_not_produce_infinite_growth(tmp_path):
    data = extract(RAW / "similarweb-google-com.html")
    fields = data["layout"]["data"]
    fields["traffic"]["visitsHistory"] = {
        "2022-10-01": 0,
        "2022-11-01": 0,
        "2022-12-01": 10,
    }
    for row, rank in zip(
        fields["ranking"]["globalRankCompetitorsHistory"]["google.com"],
        [0, 0, 2],
        strict=True,
    ):
        row["rank"] = rank
    html = tmp_path / "similarweb-zero.html"
    html.write_text(f"<script>window.__APP_DATA__ = {json.dumps(data)};</script>")
    db = tmp_path / "data.duckdb"
    pipeline(
        [html],
        ROOT / "config/schema_config.json",
        tmp_path / "csv",
        db,
        SQL / "duckdb_load.sql",
        SQL / "duckdb_transform.sql",
    )
    create_views(
        db,
        SQL / "duckdb_mom_visits.sql",
        SQL / "duckdb_mom_rank.sql",
        SQL / "duckdb_relative_scale.sql",
    )
    with duckdb.connect(str(db)) as conn:
        assert conn.execute(
            "SELECT mom_growth_percent FROM monthly_visit_changes"
        ).fetchall() == [(None,), (None,), (None,)]
        assert conn.execute(
            "SELECT rank_change_percent FROM monthly_rank_changes"
        ).fetchall() == [(None,), (None,), (None,)]
        assert conn.execute("SELECT visit_growth FROM relative_ranking").fetchone() == (
            None,
        )


@pytest.mark.parametrize("missing", [False, True])
def test_analysis_sql_errors_propagate(tmp_path, missing):
    sql_path = tmp_path / "analysis.sql"
    if not missing:
        sql_path.write_text("THIS IS INVALID SQL;")
    error = FileNotFoundError if missing else duckdb.Error
    with pytest.raises(error):
        create_views(tmp_path / "data.duckdb", sql_path, sql_path, sql_path)
