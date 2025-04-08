import requests
from pathlib import Path

import luigi
#from luigi.contrib import external_program
from luigi.contrib.spark import PySparkTask, SparkSubmitTask
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import duckdb

BASE_PATH = "./"
WORKFLOW_PATH = "workflow"


class AllTasks(luigi.WrapperTask):
    # date = luigi.DateParameter(default=datetime.date.today())
    def requires(self):
        #yield SourceDataset()
        yield PrepFilms()
        #yield GenreParquets()
        yield CosineSimilarity()
        yield CreateDuckDBFilms()
        yield CreateDuckDBFilmsCosSim()


class CreateDuckDBFilms(luigi.Task):
    """
        Import the films parquet file into a duckdb database, for the FastAPI server.
        DuckDB does support serving parquet files directly, but parquets do not
        work well with an ORM and SQLAlchemy.
    """
    db_file = luigi.Parameter(default="duckdb/films.duckdb")
    db_table = luigi.Parameter(default="films")
    ddb_conf = {'access_mode': 'READ_WRITE',
                'force_download': True,
                }
    #resources = {'overwrite_resource': 1}

    def requires(self):
        return PrepFilms()

    def input(self):
        return luigi.LocalTarget(
            Path(BASE_PATH,'output', 'films')
        )

    def output(self):
        return luigi.LocalTarget(
            #Path(BASE_PATH, self.db_file)
            Path(BASE_PATH, Path(self.db_file).parent, self.db_table + '.done')
        )

    def run(self):
        parquet_path = self.input().path
        db_file = Path(BASE_PATH, self.db_file)
        db_table = self.db_table
        #task_create_duckdb(parquet_path, db_file, self.db_table)
        with duckdb.connect(db_file, config= self.ddb_conf) as con:
            parquet_files = Path(parquet_path, "*.parquet")
            ingest_query = f'SELECT * FROM read_parquet("{parquet_files}")'
            print(f"INGEST QUERY {ingest_query}")
            con.sql(f'CREATE OR REPLACE TABLE {db_table} AS {ingest_query}')
            columns = con.execute(f"SELECT DISTINCT(name) FROM pragma_table_info('{db_table}')")\
                         .fetchall()
            print(f"{db_table} created with columns = {columns}")
            con.close()
        Path(self.output().path).touch()



class CreateDuckDBFilmsCosSim(luigi.Task):
    """
        Ingest the csfilms parquet into a duckdb database file, for the FastAPI server.
    """
    db_file = luigi.Parameter(default="duckdb/films.duckdb")
    db_table = luigi.Parameter(default="films_cos_sim")
    ddb_conf = {'access_mode': 'READ_WRITE',}

    def requires(self):
        return CosineSimilarity()

    def input(self):
        return luigi.LocalTarget(
            Path(BASE_PATH,'output', 'csfilms')
        )

    def output(self):
        # this needs to be the table in the database, not the db file
        return luigi.LocalTarget(
            Path(BASE_PATH, Path(self.db_file).parent, self.db_table + '.done')
        )

    def run(self):
        parquet_path = self.input().path
        db_file = Path(BASE_PATH, self.db_file)
        db_table = self.db_table
        with duckdb.connect(db_file, config= self.ddb_conf) as con:
            parquet_files = Path(parquet_path, "*.parquet")
            ingest_query = f'SELECT * FROM read_parquet("{parquet_files}")'
            print(f"INGEST QUERY {ingest_query}")
            con.sql(f'CREATE OR REPLACE TABLE {db_table} AS {ingest_query}')
            con.close()
        Path(self.output().path).touch()


class PrepFilms(SparkSubmitTask):
    """
    This class uses :py:meth:`luigi.contrib.spark.SparkSubmitTask.run`.
    See luigi.cfg for [spark] configuration.
    """
    priority = 90
    driver_memory = '15g'
    executor_memory = '3g'
    total_executor_cores = luigi.IntParameter(default=32, significant=False)

    name = "PySpark transform IMDb datasets to parquet file"
    app = Path(WORKFLOW_PATH, 'task_prep_films.py')

    retry_count = 0

    def requires(self):
        return [
            SourceDataset("https://datasets.imdbws.com/title.basics.tsv.gz"),
            SourceDataset("https://datasets.imdbws.com/title.principals.tsv.gz"),
            SourceDataset("https://datasets.imdbws.com/name.basics.tsv.gz"),
            SourceDataset("https://datasets.imdbws.com/title.ratings.tsv.gz")
        ]

    def input(self):
        return luigi.LocalTarget(
            Path(BASE_PATH,'datasets')
        )

    def output(self):
        return luigi.LocalTarget(
            Path(BASE_PATH, 'output', 'films')
        )

    def app_options(self):
        # These are passed to the Spark main args in the defined order.
        return [self.input().path, self.output().path]


class CosineSimilarity(SparkSubmitTask):
    """
    This class uses :py:meth:`luigi.contrib.spark.SparkSubmitTask.run`.
    See luigi.cfg for [spark] configuration.
    """
    priority = 50
    driver_memory = '32g'
    executor_memory = '2g'
    total_executor_cores = luigi.IntParameter(default=16, significant=False)

    name = "Create parquet of cosine similarity dot product of every film x all films"
    app = Path(WORKFLOW_PATH, 'task_cosine_similarity.py')

    retry_count = 0

    def requires(self):
        return PrepFilms()

    def app_options(self):
        # These are passed to the Spark main args in the defined order.
        return [self.input().path, self.output().path]

    def input(self):
        return luigi.LocalTarget(
            Path(BASE_PATH,'output', 'films')
        )

    def output(self):
        return luigi.LocalTarget(
            Path(BASE_PATH, 'output', 'csfilms')
        )


class SourceDataset(luigi.Task):
    """
        Download a file to the datasets/ directory. Used by PrepFilms().
        :param str url: is the full url of the file to download.
        :param str path: the destination path, which should be accessible to all
        worker nodes, such as an NFS mount, HDFS, S3, etc. with :py:meth:S3Target()
    """
    url = luigi.Parameter()
    path = luigi.Parameter(default="datasets")

    def output(self):
        _file = Path(self.url).name
        _output = Path(BASE_PATH, self.path, _file)
        return luigi.LocalTarget(_output)

    def run(self):
        file = self.output().path
        r = requests.get(self.url)
        with open(file, "wb") as fd:
            #for chunk in r.raw(chunk_size=4096):
            for chunk in r.iter_content(chunk_size=4096):
                fd.write(chunk)


class GenreParquets(PySparkTask):
    """
    Taking the output from stage 1, create a folder called genres, and
    inside this folder produce parquet files for each subgenre from the dataset. So
    for example, Action.parquet will contain all the die hard/mission impossible
    films, etc.

    This class uses :py:meth:`luigi.contrib.spark.PySparkTask.main`.

    """
    priority = 60
    driver_memory = '2g'
    executor_memory = '3g'
    total_executor_cores = luigi.IntParameter(default=32, significant=False)

    name = "Parition Films in to genre=Genre parquet files"
    # runtime = luigi.DateMinuteParameter(batch_method=max)

    def input(self):
        return luigi.LocalTarget(
            Path(BASE_PATH,'output', 'films')
        )
        #return S3Target("s3n://example.org/bucket/input/films")

    def requires(self):
        return PrepFilms()

    def output(self):
        return luigi.LocalTarget(
            Path(BASE_PATH,'output', 'genres')
        )
        #return S3Target('s3n://example.org/bucket/output/genres')

    def main(self, sc, *args):
        input_path = self.input().path
        output_path = self.output().path
        spark = SparkSession(sc).builder.getOrCreate()

        df = spark.read.parquet(input_path)\
            .withColumn('genre', f.col('genres')[0])\
            .repartition('genre')\
            .sortWithinPartitions('persons')

        df.write.parquet(output_path, mode='overwrite', partitionBy='genre')
        #df.write.saveAsTable('genres', mode='overwrite', partitionBy='genre')
