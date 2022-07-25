from pyspark.sql import SparkSession

from spotify_2018_top_songs import spotify_songs

__all__ = ["artists_danceability"]


def artists_danceability(spark: SparkSession):
    df = spotify_songs(spark)
    df.createOrReplaceTempView('spotify_data')
    query = """
          SELECT artists, AVG(danceability) as avg_danceability
          FROM spotify_data
          GROUP BY artists
        """
    avg_danceability_by_artist = spark.sql(query)
    avg_danceability_by_artist.show(5)
    return avg_danceability_by_artist
