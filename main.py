import argparse
from pathlib import Path

from loguru import logger

from src.analysis import run_all_analyses
from src.etl_pipeline import pipeline
from src.visualization import plot_all_analyses


@logger.catch(reraise=True)
def main(*, input_dir: Path | None = None, output_root: Path | None = None):
    source_prefix = "similarweb"
    project_root = Path(__file__).resolve().parent
    raw_data_dir = (
        input_dir if input_dir is not None else project_root / "data/raw/scraped_html"
    )
    output_root = output_root if output_root is not None else project_root

    files_to_process = sorted(raw_data_dir.glob(f"{source_prefix}*.html"))

    if not files_to_process:
        logger.warning(
            f"No HTML files found in {raw_data_dir} matching the {source_prefix}"
        )
        return

    pipeline_args = {
        "files_to_process": files_to_process,
        "schema_config_path": project_root / "config/schema_config.json",
        "output_dir": output_root / "data/transformed",
        "duckdb_file_path": output_root / "db/scraped_data.duckdb",
        "duckdb_load_sql_path": project_root / "sql/duckdb_load.sql",
        "duckdb_transform_sql_path": project_root / "sql/duckdb_transform.sql",
    }

    logger.info(f"Starting ETL Pipeline for {len(files_to_process)} files")
    pipeline(**pipeline_args)

    # Run analysis queries
    analysis_args = {
        "duckdb_file_path": output_root / "db/scraped_data.duckdb",
        "mom_visits_sql_path": project_root / "sql/duckdb_mom_visits.sql",
        "mom_rank_sql_path": project_root / "sql/duckdb_mom_rank.sql",
        "relative_scale_sql_path": project_root / "sql/duckdb_relative_scale.sql",
        "output_dir": output_root / "data/visualizations",
    }
    run_all_analyses(**analysis_args)
    plot_all_analyses(analysis_args["duckdb_file_path"], analysis_args["output_dir"])
    logger.info("Pipeline finished: extraction, transformation, analysis, and charts")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the SEO mining and analysis pipeline"
    )
    parser.add_argument(
        "--input-dir", type=Path, help="Directory of saved SimilarWeb HTML files"
    )
    parser.add_argument(
        "--output-root", type=Path, help="Root for generated data/ and db/ directories"
    )
    args = parser.parse_args()
    main(input_dir=args.input_dir, output_root=args.output_root)
