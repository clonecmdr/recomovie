"""
Task two - Stage three

Ingest the parquet files to a DuckDB database, in order to serve the data via an API backend.

SQLite might have been an alternative here, or even libSQL as embedded databases in a FastAPI.

Exposing the parquet files direclty via FastAPI or Flask presented issues.
"""
import duckdb
import sys
import logging
from pathlib import Path
#from duckdb.experimental.spark.sql import SparkSession as session
#import duckdb.experimental.spark.sql.functions as f

#spark = session.builder.getOrCreate()
log = logging.getLogger("luigi")


def task_create_duckdb(parquet_path: str = "../output/films",
                            db_file: str = "../duckdb/films.duckdb",
                            db_table: str = "films"
                    ):
    """ """
    log = logging.getLogger("luigi")
    log.info("Openning DuckDB database file {db_file}")
    ddb_conf = {'access_mode': 'READ_WRITE',
                'force_download': True,
                }
    #with duckdb.connect(db_file) as con:
    con = duckdb.connect(db_file, config = ddb_conf)

    log.info("Ingesting {parquet_path} into table name {db_table}")
    # ingest the parquet file into table
    parquet_files = Path(parquet_path, "*.parquet")
    ingest_query = f'SELECT * FROM read_parquet("{parquet_files}")'
    con.sql(f'CREATE OR REPLACE TABLE {db_table} AS {ingest_query}')

    columns = con.execute(f"SELECT DISTINCT(name) FROM pragma_table_info('{db_table}')").fetchall()
    log.info(f"{db_table} created with columns = {columns}")

    con.close()


if __name__ == "__main__":
    parquet_path = sys.argv[1]
    db_file = sys.argv[2]
    db_table = sys.argv[3]
    task_create_duckdb(parquet_path, db_file, "films")
    task_create_duckdb(parquet_path, db_file, "films_cos_sim")
