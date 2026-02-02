from pathlib import Path

from loguru import logger

from src.analysis import run_all_analyses
from src.etl_pipeline import pipeline
from src.visualization import plot_all_analyses


@logger.catch
def main():
    source_prefix = "similarweb"
    project_root = Path(__file__).resolve().parent
    raw_data_dir = project_root / "data/raw/scraped_html"

    files_to_process = list(raw_data_dir.glob(f"{source_prefix}*.html"))

    if not files_to_process:
        logger.warning(
            f"No HTML files found in {raw_data_dir} matching the {source_prefix}"
        )
        return

    pipeline_args = {
        "files_to_process": files_to_process,
        "schema_config_path": project_root / "config/schema_config.json",
        "output_dir": project_root / "data/transformed",
        "duckdb_file_path": project_root / "db/scraped_data.duckdb",
        "duckdb_load_sql_path": project_root / "sql/duckdb_load.sql",
        "duckdb_transform_sql_path": project_root / "sql/duckdb_transform.sql",
    }

    logger.info(f"Starting ETL Pipeline for {len(files_to_process)} files")
    pipeline(**pipeline_args)
    logger.info("Pipeline Finished")

    # Run analysis queries
    analysis_args = {
        "duckdb_file_path": project_root / "db/scraped_data.duckdb",
        "mom_visits_sql_path": project_root / "sql/duckdb_mom_visits.sql",
        "mom_rank_sql_path": project_root / "sql/duckdb_mom_rank.sql",
        "relative_scale_sql_path": project_root / "sql/duckdb_relative_scale.sql",
        "output_dir": project_root / "data/visualizations",
    }
    run_all_analyses(**analysis_args)
    plot_all_analyses(analysis_args["duckdb_file_path"], analysis_args["output_dir"])


if __name__ == "__main__":
    main()
