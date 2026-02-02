"""Analysis module for SEO metrics.

This module provides functions to run analytical queries on the transformed data,
including month-over-month visit growth, rank changes, and relative rankings.
"""

from pathlib import Path

import duckdb
from loguru import logger


def create_views(
    duckdb_file_path: Path,
    mom_visits_sql_path: Path,
    mom_rank_sql_path: Path,
    relative_scale_sql_path: Path,
) -> None:
    """Creates analysis views in the DuckDB database.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        mom_visits_sql_path: Path to the SQL file for MoM visits view.
        mom_rank_sql_path: Path to the SQL file for MoM rank view.
        relative_scale_sql_path: Path to the SQL file for relative ranking view.
    """
    sql_files = {
        "monthly_visit_changes": mom_visits_sql_path,
        "monthly_rank_changes": mom_rank_sql_path,
        "relative_ranking": relative_scale_sql_path,
    }

    with duckdb.connect(str(duckdb_file_path)) as conn:
        for view_name, sql_path in sql_files.items():
            if not sql_path.exists():
                logger.warning(f"SQL file not found: {sql_path}")
                continue

            with open(sql_path, "r") as f:
                sql = f.read()

            try:
                conn.execute(sql)
                logger.info(f"Created view: {view_name}")
            except Exception as e:
                logger.error(f"Failed to create view {view_name}: {e}")


def run_all_analyses(
    duckdb_file_path: Path,
    mom_visits_sql_path: Path,
    mom_rank_sql_path: Path,
    relative_scale_sql_path: Path,
    output_dir: Path,
) -> None:
    """Creates views and generates visualizations.

    Args:
        duckdb_file_path: Path to the DuckDB database file.
        mom_visits_sql_path: Path to the SQL file for MoM visits view.
        mom_rank_sql_path: Path to the SQL file for MoM rank view.
        relative_scale_sql_path: Path to the SQL file for relative ranking view.
        output_dir: Directory where visualization PNG files will be saved.
    """
    create_views(
        duckdb_file_path,
        mom_visits_sql_path,
        mom_rank_sql_path,
        relative_scale_sql_path,
    )
