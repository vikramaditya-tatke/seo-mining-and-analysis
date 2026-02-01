import re
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import orjson
import polars as pl
from loguru import logger
from selectolax.parser import HTMLParser

from src.models.data_field import DataField


def load_extraction_config(config_path: Path) -> List[DataField]:
    """Loads the extraction config from a JSON file and converts to DataField objects.

    Args:
        config_path: The path to the JSON configuration file.

    Returns:
        A list of DataField objects.

    Raises:
        FileNotFoundError: If the config file does not exist.
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = orjson.loads(f.read())

    return [DataField(**field) for field in data]


def extract(file_path: Path) -> Dict[str, Any]:
    """Extracts the window.__APP_DATA__ JSON blob from an HTML file.

    Args:
        file_path: The path to the HTML file.

    Returns:
        A dictionary containing the extracted JSON data, or an empty dictionary
        if not found or on error.
    """
    try:
        html_content = file_path.read_text(encoding="utf-8")
        tree = HTMLParser(html_content)
        for node in tree.css("script"):
            text = node.text()
            if text and "window.__APP_DATA__" in text:
                match = re.search(r"window\.__APP_DATA__\s*=\s*({.*})", text)
                if match:
                    return orjson.loads(match.group(1))

        logger.warning(f"No __APP_DATA__ found in {file_path}")
        return {}
    except (FileNotFoundError, UnicodeDecodeError, orjson.JSONDecodeError) as e:
        logger.error(f"Error processing {file_path}: {e}")
        return {}


def stage_1_polars_transform(
    raw_data: List[Dict[str, Any]], schema: List[DataField]
) -> pl.LazyFrame:
    """Uses Polars to isolate raw nested fields and perform initial scalar cleaning.

    Args:
        raw_data: A list of dictionaries containing the raw extracted data.
        schema: A list of DataField objects defining the extraction schema.

    Returns:
        A Polars LazyFrame with selected and pre-cleaned columns.
    """
    lf = pl.LazyFrame(raw_data, strict=False)

    root_expr = pl.col("layout").struct.field("data")
    selected_columns = [field.to_polars_expr(root_expr) for field in schema]

    lf = lf.select(selected_columns).with_columns(
        [
            pl.col("bounce_rate_raw")
            .str.strip_chars("%")
            .cast(pl.Float64, strict=False)
            .alias("Bounce Rate Percent"),
            (
                pl.col("Avg Visit Duration")
                .str.strptime(pl.Time, "%H:%M:%S", strict=False)
                .dt.hour()
                * 3600
                + pl.col("Avg Visit Duration")
                .str.strptime(pl.Time, "%H:%M:%S", strict=False)
                .dt.minute()
                * 60
                + pl.col("Avg Visit Duration")
                .str.strptime(pl.Time, "%H:%M:%S", strict=False)
                .dt.second()
            )
            .cast(pl.Int64)
            .alias("Avg Visit Duration (Seconds)"),
        ]
    )
    return lf


def stage_2_duckdb_transform(
    df: pl.LazyFrame, sql_file_path: Path, output_file: Path, duckdb_file_path: Path
) -> None:
    """Uses DuckDB SQL for complex nested transformations and final output.

    Args:
        df: The input Polars LazyFrame.
        sql_file_path: Path to the SQL file containing the transformation query.
        output_file: Path where the result CSV will be saved.
        duckdb_file_path: Path to the DuckDB database file.
    """
    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, "r") as file:
        query = file.read()

    logger.info("Running DuckDB Transformation")

    export_query = f"COPY ({query}) TO '{output_file}' (HEADER, DELIMITER ',')"

    duckdb_file_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(duckdb_file_path)) as conn:
        conn.register("source_data", df)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS specter_interview
            AS
            SELECT * FROM source_data;
        """)
        conn.execute(export_query)

    logger.info(f"Successfully wrote results to {output_file}")


def pipeline(
    files_to_process: List[Path],
    schema_config_path: Path,
    sql_file_path: Path,
    output_file: Path,
    duckdb_file_path: Path,
) -> None:
    """Orchestrates the ETL pipeline.

    Args:
        files_to_process: A list of paths to the files to process.
        schema_config_path: Path to the extraction configuration JSON file.
        sql_file_path: Path to the DuckDB SQL transformation file.
        output_file: Path where the result CSV will be saved.
        duckdb_file_path: Path to the DuckDB database file.
    """
    schema_config = load_extraction_config(schema_config_path)
    logger.info(f"Loaded {len(schema_config)} fields from config")

    raw_json_blobs = []

    for file in files_to_process:
        raw_json_blobs.append(extract(file))

    df_polars = stage_1_polars_transform(raw_json_blobs, schema_config)
    stage_2_duckdb_transform(
        df_polars,
        sql_file_path,
        output_file,
        duckdb_file_path,
    )
