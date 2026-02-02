"""Visualization module for SEO metrics using Altair.

This module creates individual PNG charts separated by scale for better readability.
"""

from pathlib import Path

import duckdb
from loguru import logger


def plot_mom_visits(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Creates Month-over-Month visits charts.

    Separates charts by scale: startups (stripe, crunchbase, pitchbook) on one chart,
    google on a separate chart due to massive scale difference.

    Args:
        conn: DuckDB connection.
        output_dir: Directory to save PNG charts.
    """
    import altair as alt

    startups_df = conn.execute(
        """SELECT * FROM monthly_visit_changes
           WHERE visits IS NOT NULL
           AND Domain IN ('stripe.com', 'crunchbase.com', 'pitchbook.com')
           ORDER BY month, Domain"""
    ).fetchdf()

    google_df = conn.execute(
        """SELECT * FROM monthly_visit_changes
           WHERE visits IS NOT NULL AND Domain = 'google.com'
           ORDER BY month"""
    ).fetchdf()

    if not startups_df.empty:
        chart = (
            alt.Chart(startups_df)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("visits:Q", title="Visits"),
                color=alt.Color("Domain:N", legend=alt.Legend(orient="right")),
                tooltip=[
                    alt.Tooltip("month:T", title="Month"),
                    alt.Tooltip("visits:Q", title="Visits", format=","),
                    alt.Tooltip("mom_growth_percent:Q", title="Growth %", format=".2f"),
                ],
            )
            .properties(
                title="Month-over-Month Website Visits - Startups",
                width=800,
                height=500,
            )
        )
        chart.save(str(output_dir / "mom_visits_startups.png"), scale_factor=3)
        logger.info("Saved mom_visits_startups.png")

    if not google_df.empty:
        chart = (
            alt.Chart(google_df)
            .mark_line(point=True, color="#DB4437", strokeWidth=3)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("visits:Q", title="Visits"),
                tooltip=[
                    alt.Tooltip("month:T", title="Month"),
                    alt.Tooltip("visits:Q", title="Visits", format=","),
                    alt.Tooltip("mom_growth_percent:Q", title="Growth %", format=".2f"),
                ],
            )
            .properties(
                title="Month-over-Month Website Visits - Google",
                width=800,
                height=500,
            )
        )
        chart.save(str(output_dir / "mom_visits_google.png"), scale_factor=3)
        logger.info("Saved mom_visits_google.png")


def plot_mom_rank(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Creates Month-over-Month rank charts.

    Separates charts by scale: startups (stripe, crunchbase, pitchbook) on one chart,
    google on separate chart (rank 1), byte-trading on separate chart (highly volatile).

    Args:
        conn: DuckDB connection.
        output_dir: Directory to save PNG charts.
    """
    import altair as alt

    startups_df = conn.execute(
        """SELECT * FROM monthly_rank_changes
           WHERE Domain IN ('stripe.com', 'crunchbase.com', 'pitchbook.com')
           ORDER BY month, Domain"""
    ).fetchdf()

    google_df = conn.execute(
        """SELECT * FROM monthly_rank_changes
           WHERE Domain = 'google.com'
           ORDER BY month"""
    ).fetchdf()

    byte_df = conn.execute(
        """SELECT * FROM monthly_rank_changes
           WHERE Domain = 'byte-trading.com'
           ORDER BY month"""
    ).fetchdf()

    if not startups_df.empty:
        chart = (
            alt.Chart(startups_df)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("rank:Q", title="Global Rank", scale=alt.Scale(reverse=True)),
                color=alt.Color("Domain:N", legend=alt.Legend(orient="right")),
                tooltip=[
                    alt.Tooltip("month:T", title="Month"),
                    alt.Tooltip("rank:Q", title="Rank", format=","),
                    alt.Tooltip("rank_change:Q", title="Change", format="+,"),
                    alt.Tooltip(
                        "rank_change_percent:Q", title="Change %", format=".2f"
                    ),
                ],
            )
            .properties(
                title="Month-over-Month Global Rank - Startups", width=800, height=500
            )
        )
        chart.save(str(output_dir / "mom_rank_startups.png"), scale_factor=3)
        logger.info("Saved mom_rank_startups.png")

    if not google_df.empty:
        chart = (
            alt.Chart(google_df)
            .mark_line(point=True, color="#DB4437", strokeWidth=3)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("rank:Q", title="Global Rank", scale=alt.Scale(reverse=True)),
                tooltip=[
                    alt.Tooltip("month:T", title="Month"),
                    alt.Tooltip("rank:Q", title="Rank", format=","),
                    alt.Tooltip("rank_change:Q", title="Change", format="+,"),
                ],
            )
            .properties(
                title="Month-over-Month Global Rank - Google", width=800, height=500
            )
        )
        chart.save(str(output_dir / "mom_rank_google.png"), scale_factor=3)
        logger.info("Saved mom_rank_google.png")

    if not byte_df.empty:
        chart = (
            alt.Chart(byte_df)
            .mark_line(point=True, color="#4285F4", strokeWidth=3)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("rank:Q", title="Global Rank", scale=alt.Scale(reverse=True)),
                tooltip=[
                    alt.Tooltip("month:T", title="Month"),
                    alt.Tooltip("rank:Q", title="Rank", format=","),
                    alt.Tooltip("rank_change:Q", title="Change", format="+,"),
                    alt.Tooltip(
                        "rank_change_percent:Q", title="Change %", format=".2f"
                    ),
                ],
            )
            .properties(
                title="Month-over-Month Global Rank - Byte-trading",
                width=800,
                height=500,
            )
        )
        chart.save(str(output_dir / "mom_rank_byte_trading.png"), scale_factor=3)
        logger.info("Saved mom_rank_byte_trading.png")


def plot_relative_ranking(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Creates horizontal bar charts for relative ranking comparison.

    Displays three metrics: visit growth percentage, rank change (positive=better),
    and combined score (lower=better).

    Args:
        conn: DuckDB connection.
        output_dir: Directory to save PNG charts.
    """
    import altair as alt

    df = conn.execute(
        """SELECT Domain, visit_growth, rank_change, combined_rank
           FROM relative_ranking ORDER BY combined_rank"""
    ).fetchdf()

    visit_chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("visit_growth:Q", title="Visit Growth %"),
            y=alt.Y("Domain:N", sort=None, title=""),
            color=alt.Color(
                "visit_growth:Q",
                scale=alt.Scale(scheme="redyellowgreen"),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Domain:N"),
                alt.Tooltip("visit_growth:Q", format=".2f"),
            ],
        )
        .properties(title="Visit Growth by Domain", width=600, height=200)
    )

    rank_chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("rank_change:Q", title="Rank Change (Positive=Better)"),
            y=alt.Y("Domain:N", sort=None, title=""),
            color=alt.Color(
                "rank_change:Q",
                scale=alt.Scale(scheme="redyellowgreen"),
                legend=None,
            ),
            tooltip=[alt.Tooltip("Domain:N"), alt.Tooltip("rank_change:Q", format=",")],
        )
        .properties(title="Rank Change by Domain", width=600, height=200)
    )

    score_chart = (
        alt.Chart(df.sort_values("combined_rank", ignore_index=True))
        .mark_bar()
        .encode(
            x=alt.X("combined_rank:Q", title="Combined Score (Lower is Better)"),
            y=alt.Y("Domain:N", sort=None, title=""),
            color=alt.Color(
                "combined_rank:Q",
                scale=alt.Scale(scheme="viridis", reverse=True),
                legend=None,
            ),
            tooltip=[alt.Tooltip("Domain:N"), alt.Tooltip("combined_rank:Q")],
        )
        .properties(title="Combined Score by Domain", width=600, height=200)
    )

    chart = alt.vconcat(visit_chart, rank_chart, score_chart, spacing=20).properties(
        title="Relative Ranking Summary", padding=20
    )
    chart.save(str(output_dir / "relative_ranking.png"), scale_factor=3)
    logger.info("Saved relative_ranking.png")


def plot_all_analyses(duckdb_file_path: Path, output_dir: Path) -> None:
    """Creates all PNG visualization charts.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        output_dir: Directory where PNG charts will be saved.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(duckdb_file_path)) as conn:
        plot_mom_visits(conn, output_dir)
        plot_mom_rank(conn, output_dir)
        plot_relative_ranking(conn, output_dir)
