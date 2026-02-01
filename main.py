import sys
from pathlib import Path

import orjson
from loguru import logger

from src.etl_pipeline import pipeline


def concise_log_sink(message):
    record = message.record
    subset = {
        "time": record["time"].isoformat(),
        "message": record["message"],
        "level": record["level"].name,
        "file": record["file"].name,
        "line": record["line"],
    }
    sys.stderr.write(orjson.dumps(subset).decode("utf-8") + "\n")


logger.remove()
logger.add(concise_log_sink)


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
        "sql_file_path": project_root / "sql/duckdb_transform.sql",
        "output_file": project_root / "data/transformed/specter_interview_task.csv",
        "duckdb_file_path": project_root / "db/specter_interview_task.duckdb",
    }

    logger.info(f"Starting ETL Pipeline for {len(files_to_process)} files")
    pipeline(**pipeline_args)
    logger.info("Pipeline Finished")


if __name__ == "__main__":
    main()
