"""
Task two

The client requires a function to detect similarity between films. The function will take in a
film’s `film_id`, and a `threshold percentage` as input, and will return a `dataframe` that contains all
films with a similarity percentage above the threshold. The way similarity is calculated is up
to you, but the output should be sensible. 
(For example, any star wars film should be similar to all other star wars films, or films by the 
same director have a similar style etc.)
"""
import sys
from os import path
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import Normalizer


if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # load the IMDb films data prepared in previous task
    df = spark.read.parquet(input_path)

    # Choose which columns we will calculate the cosine similarity index for
    feature_cols = ['title', 'genres', 'rating', 'persons']

    ## Another approach -- calculate the cosine similarity across all rows.
    # The Jaccard Index or Euclidean distance would have been good alternatives.
    # Using the ml package to compute the L2 norm of the TF-IDF of every row,
    # then multiply the table with itself to get the cosine similarity
    # as the dot product of the L^2 norms:

    # concatenate the elements of the feature columns in to one long array
    df_film = df.withColumn("features", f.split(f.concat_ws(", ", *feature_cols), ',' ))

    # drop all the original columnss from the df_film dataframe
    drop_cols = [c for c in df.columns if c != 'film_id']
    df_film = df_film.drop(*drop_cols)

    # Compute the TF-IDF (term freq inverse doc freq)
    # The inputCol must be an array
    hashingTF = HashingTF(inputCol='features', outputCol="features_tf")

    # numFeatures: Number of features. Should be greater than 0. (default: 262144)
    #hashingTF.setNumFeatures(1000)

    tf = hashingTF.transform(df_film)
    idf = IDF(inputCol="features_tf", outputCol="features_tfidf").fit(tf)

    # we can also drop the text list of features and the TF column, and repartition to 32
    tfidf = idf.transform(tf).drop('features').drop('features_tf')

    # Calculate the L2 norm
    normalizer = Normalizer(inputCol="features_tfidf", outputCol="norm")
    data = normalizer.transform(tfidf).drop('features_tfidf')

    #data = data.repartitionByRange(32, 'film_id')

    # Compute the dot product Cross Join or Cosine Similarity
    # and write the our similarity index to a parquet file
    sc.setLogLevel("INFO")

    # A UDF function for dot product to use in the cross join below
    dot_udf = f.udf(lambda x, y: float(x.dot(y)), DoubleType())

    # check the output path doesn't already exist, before wasting lots of compute time
    if not path.exists(output_path):
        data.alias('i').join(data.alias('j'), f.col('i.film_id') < f.col('j.film_id'))\
            .select(
                f.col('i.film_id').alias('film_id'),
                f.col('j.film_id').alias('other_id'),
                dot_udf("i.norm", "j.norm").alias("similarity"))\
            .sort('film_id', f.desc('similarity'))\
            .write.parquet(output_path, mode='error')
