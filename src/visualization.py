"""Visualization module for SEO metrics using Altair.

This module creates individual PNG charts separated by scale for better readability.
"""

from pathlib import Path

import altair as alt
import duckdb
from loguru import logger


def save_monthly_chart(
    chart: alt.Chart, output_path: Path, growth_field: str, growth_title: str
) -> None:
    """Save levels and monthly growth together so PNGs expose both metrics."""
    chart = chart.encode(
        x=alt.X(
            "month:T",
            title="Month",
            scale=alt.Scale(type="utc"),
            axis=alt.Axis(format="%b %Y", tickCount="month", labelAngle=0),
        )
    )
    growth_chart = chart.encode(
        y=alt.Y(f"{growth_field}:Q", title="Change (%)", scale=alt.Scale(zero=True)),
        tooltip=["month:T", "domain:N", alt.Tooltip(f"{growth_field}:Q", format=".2f")],
    ).properties(title=growth_title, height=220)
    alt.vconcat(chart, growth_chart, spacing=30).resolve_scale(y="independent").save(
        output_path, scale_factor=3
    )


def plot_mom_visits(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Creates Month-over-Month visits charts.

    Separates charts by scale: startups (stripe, crunchbase, pitchbook) on one chart,
    google on a separate chart due to massive scale difference.

    Args:
        conn: DuckDB connection.
        output_dir: Directory to save PNG charts.
    """
    startups_df = conn.execute(
        """SELECT * FROM monthly_visit_changes
           WHERE visits IS NOT NULL
           AND domain IN ('stripe.com', 'crunchbase.com', 'pitchbook.com')
           ORDER BY month, domain"""
    ).fetchdf()

    google_df = conn.execute(
        """SELECT * FROM monthly_visit_changes
           WHERE visits IS NOT NULL AND domain = 'google.com'
           ORDER BY month"""
    ).fetchdf()

    if not startups_df.empty:
        save_monthly_chart(
            alt.Chart(startups_df)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x="month:T",
                y=alt.Y("visits:Q", scale=alt.Scale(zero=True)),
                color=alt.Color("domain:N", legend=alt.Legend(orient="right")),
                tooltip=["month", "domain", "visits", "mom_growth_percent:Q"],
            )
            .properties(
                title="Month-over-Month Website Visits - Startups",
                width=800,
                height=500,
            ),
            output_dir / "mom_visits_startups.png",
            "mom_growth_percent",
            "Month-over-Month Visit Growth (%)",
        )
        logger.info("Saved mom_visits_startups.png")

    if not google_df.empty:
        save_monthly_chart(
            alt.Chart(google_df)
            .mark_line(point=True, color="#DB4437", strokeWidth=3)
            .encode(
                x="month:T",
                y="visits:Q",
                tooltip=["month", "visits", "mom_growth_percent:Q"],
            )
            .properties(
                title="Month-over-Month Website Visits - Google", width=800, height=500
            ),
            output_dir / "mom_visits_google.png",
            "mom_growth_percent",
            "Month-over-Month Visit Growth (%)",
        )
        logger.info("Saved mom_visits_google.png")


def plot_mom_rank(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Creates Month-over-Month rank charts.

    Separates charts by scale: startups (stripe, crunchbase, pitchbook) on one chart,
    google on separate chart (rank 1), byte-trading on separate chart (highly volatile).

    Args:
        conn: DuckDB connection.
        output_dir: Directory to save PNG charts.
    """
    startups_df = conn.execute(
        """SELECT * FROM monthly_rank_changes
           WHERE domain IN ('stripe.com', 'crunchbase.com', 'pitchbook.com')
           ORDER BY month, domain"""
    ).fetchdf()

    google_df = conn.execute(
        """SELECT * FROM monthly_rank_changes
           WHERE domain = 'google.com'
           ORDER BY month"""
    ).fetchdf()

    byte_df = conn.execute(
        """SELECT * FROM monthly_rank_changes
           WHERE domain = 'byte-trading.com'
           ORDER BY month"""
    ).fetchdf()

    if not startups_df.empty:
        save_monthly_chart(
            alt.Chart(startups_df)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x="month:T",
                y=alt.Y("rank:Q", scale=alt.Scale(reverse=True)),
                color=alt.Color("domain:N", legend=alt.Legend(orient="right")),
                tooltip=[
                    "month",
                    "domain",
                    "rank",
                    "rank_change",
                    "rank_change_percent:Q",
                ],
            )
            .properties(
                title="Month-over-Month Global Rank - Startups", width=800, height=500
            ),
            output_dir / "mom_rank_startups.png",
            "rank_change_percent",
            "Month-over-Month Numerical Rank Change (%, lower is better)",
        )
        logger.info("Saved mom_rank_startups.png")

    if not google_df.empty:
        save_monthly_chart(
            alt.Chart(google_df)
            .mark_line(point=True, color="#DB4437", strokeWidth=3)
            .encode(
                x="month:T",
                y=alt.Y("rank:Q", scale=alt.Scale(reverse=True)),
                tooltip=["month", "rank", "rank_change"],
            )
            .properties(
                title="Month-over-Month Global Rank - Google", width=800, height=500
            ),
            output_dir / "mom_rank_google.png",
            "rank_change_percent",
            "Month-over-Month Numerical Rank Change (%, lower is better)",
        )
        logger.info("Saved mom_rank_google.png")

    if not byte_df.empty:
        save_monthly_chart(
            alt.Chart(byte_df)
            .mark_line(point=True, color="#4285F4", strokeWidth=3)
            .encode(
                x="month:T",
                y=alt.Y("rank:Q", scale=alt.Scale(reverse=True)),
                tooltip=["month", "rank", "rank_change", "rank_change_percent:Q"],
            )
            .properties(
                title="Month-over-Month Global Rank - Byte-trading",
                width=800,
                height=500,
            ),
            output_dir / "mom_rank_byte_trading.png",
            "rank_change_percent",
            "Month-over-Month Numerical Rank Change (%, lower is better)",
        )
        logger.info("Saved mom_rank_byte_trading.png")


def plot_relative_ranking(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Creates horizontal bar charts for relative ranking comparison.

    Displays three metrics: visit growth percentage, rank change (positive=better),
    and combined score (lower=better).

    Args:
        conn: DuckDB connection.
        output_dir: Directory to save PNG charts.
    """
    df = conn.execute(
        """SELECT domain, visit_growth, rank_change, combined_rank
           FROM relative_ranking ORDER BY combined_rank"""
    ).fetchdf()

    visit_growth_df = df[df["visit_growth"].notna()]

    visit_chart = (
        alt.Chart(visit_growth_df)
        .transform_calculate(
            sign="datum.visit_growth > 0 ? 1 : datum.visit_growth < 0 ? -1 : 0"
        )
        .mark_bar()
        .encode(
            x="visit_growth:Q",
            y=alt.Y("domain:N", sort="-x"),
            color=alt.Color(
                "sign:O",
                scale=alt.Scale(
                    domain=[-1, 0, 1], range=["#e74c3c", "#f1c40f", "#2ecc71"]
                ),
                legend=None,
            ),
            tooltip=["domain", "visit_growth:Q"],
        )
        .properties(title="Visit Growth by Domain", width=600, height=200)
    )

    rank_normal_df = df[abs(df["rank_change"]) < 1000000]
    rank_extreme_df = df[abs(df["rank_change"]) >= 1000000]

    rank_chart = (
        alt.Chart(rank_normal_df)
        .transform_calculate(
            sign="datum.rank_change > 0 ? 1 : datum.rank_change < 0 ? -1 : 0"
        )
        .mark_bar()
        .encode(
            x="rank_change:Q",
            y=alt.Y("domain:N", sort="-x"),
            color=alt.Color(
                "sign:O",
                scale=alt.Scale(
                    domain=[-1, 0, 1], range=["#e74c3c", "#f1c40f", "#2ecc71"]
                ),
                legend=None,
            ),
            tooltip=["domain", "rank_change"],
        )
        .properties(
            title="Rank Change by Domain (excluding extreme values)",
            width=600,
            height=200,
        )
    )

    score_chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="combined_rank:Q",
            y=alt.Y("domain:N", sort="x"),
            color=alt.Color(
                "combined_rank:Q",
                scale=alt.Scale(scheme="redyellowgreen", reverse=True),
                legend=None,
            ),
            tooltip=["domain", "combined_rank"],
        )
        .properties(title="Combined Score by Domain", width=600, height=200)
    )

    charts = [visit_chart, rank_chart, score_chart]
    if not rank_extreme_df.empty:
        rank_extreme_chart = (
            alt.Chart(rank_extreme_df)
            .transform_calculate(
                sign="datum.rank_change > 0 ? 1 : datum.rank_change < 0 ? -1 : 0"
            )
            .mark_bar()
            .encode(
                x="rank_change:Q",
                y=alt.Y("domain:N", sort="-x"),
                color=alt.Color(
                    "sign:O",
                    scale=alt.Scale(
                        domain=[-1, 0, 1], range=["#e74c3c", "#f1c40f", "#2ecc71"]
                    ),
                    legend=None,
                ),
                tooltip=["domain", "rank_change"],
            )
            .properties(title="Rank Change - Extreme Values", width=600, height=100)
        )
        charts.insert(2, rank_extreme_chart)

    alt.vconcat(*charts, spacing=20).properties(
        title="Relative Ranking Summary", padding=20
    ).save(output_dir / "relative_ranking.png", scale_factor=3)
    logger.info("Saved relative_ranking.png")


def plot_all_analyses(duckdb_file_path: Path, output_dir: Path) -> None:
    """Creates all PNG visualization charts.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        output_dir: Directory where PNG charts will be saved.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(duckdb_file_path) as conn:
        plot_mom_visits(conn, output_dir)
        plot_mom_rank(conn, output_dir)
        plot_relative_ranking(conn, output_dir)
