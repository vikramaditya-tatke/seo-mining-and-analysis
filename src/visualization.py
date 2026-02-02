"""Visualization module for SEO metrics analysis.

This module provides functions to create visualizations using Plotly Express
for the analysis results.
"""

from pathlib import Path

import duckdb
import plotly.express as px
import plotly.graph_objects as go
from loguru import logger
from plotly.subplots import make_subplots


def plot_mom_visits(duckdb_file_path: Path, output_path: Path) -> None:
    """Creates and saves a line chart for month-over-month visit growth.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        output_path: Path where the PNG file will be saved.
    """
    query = "SELECT * FROM monthly_visit_changes"

    with duckdb.connect(str(duckdb_file_path)) as conn:
        df = conn.execute(query).pl()

    if df.is_empty():
        logger.warning("No data available for MoM visits visualization")
        return

    df_pandas = df.to_pandas()

    fig = px.line(
        df_pandas,
        x="month",
        y="visits",
        color="Domain",
        title="Month-over-Month Website Visits",
        markers=True,
        labels={"visits": "Visits", "month": "Month", "Domain": "Domain"},
        hover_data=["mom_growth_percent"],
    )

    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Visits",
        hovermode="x unified",
        template="plotly_white",
        height=600,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(output_path), scale=2)
    logger.info(f"Saved MoM visits chart to {output_path}")


def plot_mom_rank(duckdb_file_path: Path, output_path: Path) -> None:
    """Creates and saves a line chart for month-over-month rank changes.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        output_path: Path where the PNG file will be saved.
    """
    query = "SELECT * FROM monthly_rank_changes"

    with duckdb.connect(str(duckdb_file_path)) as conn:
        df = conn.execute(query).pl()

    if df.is_empty():
        logger.warning("No data available for MoM rank visualization")
        return

    df_pandas = df.to_pandas()

    fig = px.line(
        df_pandas,
        x="month",
        y="rank",
        color="Domain",
        title="Month-over-Month Global Rank (Lower is Better)",
        markers=True,
        labels={"rank": "Global Rank", "month": "Month", "Domain": "Domain"},
        hover_data=["rank_change", "rank_change_percent"],
    )

    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Global Rank",
        yaxis={"autorange": "reversed"},
        hovermode="x unified",
        template="plotly_white",
        height=600,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(output_path), scale=2)
    logger.info(f"Saved MoM rank chart to {output_path}")


def plot_relative_ranking(duckdb_file_path: Path, output_path: Path) -> None:
    """Creates and saves a bar chart for relative ranking scores.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        output_path: Path where the PNG file will be saved.
    """
    query = "SELECT * FROM relative_ranking"

    with duckdb.connect(str(duckdb_file_path)) as conn:
        df = conn.execute(query).pl()

    if df.is_empty():
        logger.warning("No data available for relative ranking visualization")
        return

    df_pandas = df.to_pandas()

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Visit Growth %",
            "Rank Change",
            "Individual Rankings",
            "Combined Score (lower is better)",
        ),
    )

    fig.add_trace(
        go.Bar(
            x=df_pandas["Domain"],
            y=df_pandas["Visit Growth %"],
            name="Visit Growth %",
            marker_color="lightblue",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=df_pandas["Domain"],
            y=df_pandas["Rank Change"],
            name="Rank Change",
            marker_color="lightcoral",
        ),
        row=1,
        col=2,
    )

    fig.add_trace(
        go.Bar(
            x=df_pandas["Domain"],
            y=df_pandas["Visits Rank"],
            name="Visits Rank",
            marker_color="lightblue",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=df_pandas["Domain"],
            y=df_pandas["Rank Improvement Rank"],
            name="Rank Improvement Rank",
            marker_color="lightcoral",
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=df_pandas["Domain"],
            y=df_pandas["Combined Rank (lower is better)"],
            name="Combined Score",
            marker_color="lightgreen",
            showlegend=False,
        ),
        row=2,
        col=2,
    )

    fig.update_layout(
        title_text="Relative SEO Performance Analysis",
        template="plotly_white",
        height=800,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(output_path), scale=2)
    logger.info(f"Saved relative ranking chart to {output_path}")


def plot_all_analyses(
    duckdb_file_path: Path,
    output_dir: Path,
) -> None:
    """Creates and saves all visualization charts.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        output_dir: Directory where PNG files will be saved.
    """
    plot_mom_visits(duckdb_file_path, output_dir / "mom_visits.png")
    plot_mom_rank(duckdb_file_path, output_dir / "mom_rank.png")
    plot_relative_ranking(duckdb_file_path, output_dir / "relative_ranking.png")
