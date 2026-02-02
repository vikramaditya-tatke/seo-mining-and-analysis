import re
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Optional

import duckdb
import orjson
import polars as pl
from loguru import logger
from selectolax.parser import HTMLParser

from src.models.data_field import DataField


@contextmanager
def benchmark(name: str):
    """Context manager for timing and logging execution duration.

    Args:
        name: A descriptive name for the operation being benchmarked.

    Yields:
        None

    Example:
        with benchmark("data_processing"):
            process_data()
    """
    start = perf_counter()
    yield
    elapsed = perf_counter() - start
    logger.info(f"BENCHMARK: {name} took {elapsed:.4f} seconds")


def load_extraction_config(config_path: Path) -> list[DataField]:
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


def extract(file_path: Path) -> Optional[dict]:
    """Extracts the window.__APP_DATA__ JSON blob from an HTML file.

    This function parses the HTML content to find the specific script tag containing
    'window.__APP_DATA__' and extracts the JSON object assigned to it.

    Args:
        file_path: The Path object pointing to the HTML file.

    Returns:
        A dictionary containing the parsed JSON data.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the __APP_DATA__ pattern is not found.
        orjson.JSONDecodeError: If the extracted JSON is invalid.
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
    except (FileNotFoundError, UnicodeDecodeError, orjson.JSONDecodeError) as e:
        logger.error(f"Error processing {file_path}: {e}")


def stage_1_polars_transform(
    raw_data: list[dict], schema: list[DataField], csv_output_path: Path
):
    """Uses Polars to isolate raw nested fields and perform initial scalar cleaning.
    Writes the transformed data to a CSV file.

    Args:
        raw_data: A list of dictionaries containing the raw extracted data.
        schema: A list of DataField objects defining the extraction schema.
        csv_output_path: Path where the intermediate CSV file will be written.

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
            pl.col("Avg Visit Duration")
            .str.to_time("%H:%M:%S", strict=False)
            .pipe(lambda t: t.dt.hour() * 3600 + t.dt.minute() * 60 + t.dt.second())
            .cast(pl.Int64)
            .alias("Avg Visit Duration (Seconds)"),
        ]
    )
    complex_columns = [
        col
        for col, dtype in lf.collect_schema().items()
        if dtype in (pl.List, pl.Struct)
    ]
    lf = lf.with_columns(
        [
            pl.col(col).map_elements(
                lambda x: orjson.dumps(
                    x.to_list() if hasattr(x, "to_list") else x
                ).decode("utf-8")
                if x is not None
                else None,
                return_dtype=pl.String,
            )
            for col in complex_columns
        ]
    )
    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    lf.collect().write_csv(csv_output_path)
    logger.info(f"Stage 1: Wrote intermediate CSV to {csv_output_path}")


def load_to_duckdb(
    load_query_duckdb_file_path: Path, csv_input_file: Path, duckdb_file_path: Path
) -> None:
    """Loads CSV data into DuckDB and creates the source_data table.

    Args:
        load_query_duckdb_file_path: Path to the SQL file containing the load query.
        csv_input_file: Path to the CSV file to be loaded into DuckDB.
        duckdb_file_path: Path to the DuckDB database file.
    """
    if not load_query_duckdb_file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {load_query_duckdb_file_path}")

    with open(load_query_duckdb_file_path, "r") as file:
        load_query = file.read()
        load_query = load_query.replace("{csv_path}", str(csv_input_file))

    logger.info("Loading CSV into DuckDB")

    with duckdb.connect(str(duckdb_file_path)) as conn:
        conn.execute(load_query)

    logger.info("Successfully loaded data into DuckDB table")


def transform_in_duckdb(
    transform_query_duckdb_file_path: Path, duckdb_file_path: Path
) -> None:
    """Transforms loaded data in DuckDB using complex nested transformations.

    Args:
        transform_query_duckdb_file_path: Path to the SQL file containing the
            transformation query.
        duckdb_file_path: Path to the DuckDB database file.
    """
    if not transform_query_duckdb_file_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {transform_query_duckdb_file_path}"
        )

    with open(transform_query_duckdb_file_path, "r") as file:
        transform_query = file.read()

    logger.info("Transforming data in DuckDB")

    with duckdb.connect(str(duckdb_file_path)) as conn:
        conn.execute(transform_query)

    logger.info("Successfully loaded data into DuckDB table")


def pipeline(
    files_to_process: list[Path],
    schema_config_path: Path,
    output_dir: Path,
    duckdb_file_path: Path,
    duckdb_load_sql_path: Path,
    duckdb_transform_sql_path: Path,
) -> None:
    """Orchestrates the ETL pipeline for processing HTML files.

    The pipeline consists of three stages:
    1. Extract JSON data from HTML files using selectolax
    2. Transform and clean data using Polars, outputting to CSV
    3. Load CSV into DuckDB and apply final transformations

    Args:
        files_to_process: A list of paths to HTML files to process.
        schema_config_path: Path to the extraction configuration JSON file.
        output_dir: Directory where intermediate and output files will be saved.
        duckdb_file_path: Path to the DuckDB database file.
        duckdb_load_sql_path: Path to the SQL file for loading data into DuckDB.
        duckdb_transform_sql_path: Path to the SQL file for transforming data in DuckDB.
    """
    schema_config = load_extraction_config(schema_config_path)
    logger.info(f"Loaded {len(schema_config)} fields from config")

    raw_json_blobs = []

    for file in files_to_process:
        raw_json_blobs.append(extract(file))

    output_csv = output_dir / "polars_dump.csv"

    with benchmark("stage_1_polars_transform"):
        stage_1_polars_transform(raw_json_blobs, schema_config, output_csv)

    with benchmark("stage_2_load_to_duckdb"):
        load_to_duckdb(
            duckdb_load_sql_path,
            output_csv,
            duckdb_file_path,
        )

    with benchmark("stage_3_transform_in_duckdb"):
        transform_in_duckdb(
            duckdb_transform_sql_path,
            duckdb_file_path,
        )
