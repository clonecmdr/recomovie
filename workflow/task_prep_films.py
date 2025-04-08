"""
    Task one - stage 1

    Prep all the IMdb TSV.gz datasets.
    Import the input dataset, convert the data types, then save as a parquet file.

"""
import sys
from pathlib import Path
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    # "datasets/" if sys.argv[1] is None else sys.argv[1]
    input_path = sys.argv[1] or "datasets"
    output_path = sys.argv[2] or str(Path('output', 'films'))

    path = str(Path(input_path, "name.basics.tsv.gz"))
    schema = '''nconst STRING, primaryName STRING, birthYear DATE, deathYear DATE,
            primaryProfession STRING, knownForTitles STRING'''
    names = spark.read.csv(path, schema=schema, sep="\t", header=True, dateFormat="yyyy")
    names = names.drop('birthYear').drop('deathYear').drop('primaryProfession').drop('knownForTitles')

    # 1.5 million move ratings
    path = str(Path(input_path, "title.ratings.tsv.gz"))
    schema = 'tconst STRING not null, averageRating DECIMAL(4,2), numVotes INTEGER'
    ratings = spark.read.csv(path, schema=schema, sep="\t", header=True)

    # 10,248,884 rows in principals
    path = str(Path(input_path, "title.principals.tsv.gz"))
    schema = 'tconst STRING, ordering INTEGER, nconst STRING, category STRING, job STRING, characters STRING'
    principals = spark.read.csv(path, schema=schema, sep="\t", header=True)\
                .select('tconst', 'nconst', 'ordering', 'job')
    # join `pricipals` to `names` to get a list of people involved in the film
    principals = principals.join(names, on='nconst')
    # Perform a reverse explode, collect_set(), do not sort_array() of persons
    # principals becomes a key-val DF of tconst:[persons]
    principals = principals.groupby('tconst').agg( f.collect_set('primaryName').alias('persons') )
    # principals is 61_278_693 rows, highest ordering value is 75,
    # reduced to about #10_248_884 when collect_set() to an array of persons

    path = str(Path(input_path, "title.basics.tsv.gz"))
    schema = 'tconst STRING not null, titleType STRING, primaryTitle STRING, originalTitle STRING, isAdult BOOLEAN, startYear DATE, endYear DATE, runtimeMinutes INTEGER, genres STRING'
    titles = spark.read.csv(path, schema=schema, sep="\t", header=True, dateFormat="yyyy")\
            .select('tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres')
    # Join the ratings to the titles on tconst -> 11_297_218 1_512_899 titles
    titles = titles.join(ratings, on='tconst')

    # A bit arbitrary, but only keeep movies (not shorts, etc) with a rating above 6.0
    # Filter the number of films down to something more managable
    titles = titles.filter( (titles['titleType'] == 'movie')
                            & (titles['genres'] != '\\N')
                            & (titles['averageRating'] >= 6)
                            & (ratings['numVotes'] >= 500)
                            & (titles['runtimeMinutes'].isNotNull())
                        ).drop('titleType')
    # reduced to about 28170 movies

    # Convert genres to an array of geners for each title
    titles = titles.withColumn("genres", f.split(titles['genres'], r'\s*,\s*'))

    # add the principals (persons) array for each film
    # join principles now with persons (directors, artists, actors) to titles
    titles = titles.join(principals, on='tconst')

    # Sanitise the column names
    col_names = {'tconst': 'film_id', 
                'primaryTitle': 'title', 
                'startYear': 'year', 
                'runtimeMinutes': 'duration', 
                'primaryName': 'person',
                'averageRating': 'rating', 
                'numVotes': 'vote_count'}
    titles = titles.withColumnsRenamed(col_names)

    # strip the leading "tt" from the film_id
    titles = titles\
        .withColumn('film_id',
                    f.replace( titles.film_id, f.lit('tt'), f.lit('') ).cast("int"))

    print( titles.schema.json() )

    # Write the processed IMDB data to a parquet file
    titles.write.parquet(output_path, mode='overwrite')
