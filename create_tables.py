import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, monotonically_increasing_id, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, weekofyear, dayofweek
from pyspark.sql.types import IntegerType

from utils import quality_check

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates spark session
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_bgg_details(spark, input_data, output_data):
    """
    Processes the BoardGameGeek board game data and writes
    the data for the BGGDetails table to the output directory.

    Args:
        spark: spark session
        input_data: data directory
        output_data: directory to save data to

    Returns:
        None
    """
    # get filepath and read data
    bgg_games_data = os.path.join(input_data, "bgg/games.csv")
    bgg_games_df = spark.read.csv(bgg_games_data, header=True, inferSchema=True)

    # convert column types
    bgg_games_df = bgg_games_df.withColumn(
        "datetime_extracted",
        to_timestamp(col("datetime_extracted"), format="dd/MM/yyyy HH:mm:ss"),
    ).withColumn("year_published", col("year_published").cast(IntegerType()))

    # rename columns to match BGGDetails Table
    bgg_games_df = (
        bgg_games_df.withColumnRenamed("bgg_id", "bgg_game_id")
        .withColumnRenamed("maxplayers", "max_players")
        .withColumnRenamed("maxplaytime", "max_playtime")
        .withColumnRenamed("age", "min_age")
        .withColumnRenamed("minplayers", "min_players")
        .withColumnRenamed("minplaytime", "min_playtime")
        .withColumnRenamed("name", "game_name")
        .withColumnRenamed("users_rated", "num_ratings")
    )

    # drop PK duplicates and missing values
    bgg_games_df = bgg_games_df.dropDuplicates(["bgg_game_id"]).dropna(
        subset=["bgg_game_id"]
    )
    
    # quality check and write data
    check = quality_check(spark, bgg_games_df, ["bgg_game_id"], 18, "BGGDetails")
    assert check
    bgg_games_df.write.parquet(
        os.path.join(output_data, "bgg_games.parquet"),
        partitionBy=["year_published"],
        mode="overwrite",
    )

    
def process_atlas_users(spark, input_data, output_data):
    """
    Processes the Board Game Atlas user data and writes
    the data for the AtlasUsers table to the output directory.

    Args:
        spark: spark session
        input_data: data directory
        output_data: directory to save data to

    Returns:
        None
    """
    # get filepath and read data for the first data source
    atlas_users1_data = os.path.join(input_data, "atlas/json_user.json")
    atlas_users1_df = spark.read.json(atlas_users1_data, multiLine=True)

    # select columns of interest and rename columns to match Table
    atlas_users1_df = atlas_users1_df.select(
        col("username").alias("user_name"),
        "url",
        "description",
        col("gold").alias("atlas_gold"),
        col("experience").alias("atlas_exp"),
        col("level").alias("atlas_level"),
    )

    # drop duplicates and missing values corresponding with the table PK
    atlas_users1_df = atlas_users1_df.dropDuplicates(["user_name"]).dropna(
        subset=["user_name"]
    )

    # get filepath and read data for the second data source
    atlas_users2_data = os.path.join(input_data, "atlas/users/*.json")
    atlas_users2_df = spark.read.json(atlas_users2_data, multiLine=True)

    # only select columns of interest
    atlas_users2_df = atlas_users2_df.select(
        col("id").alias("atlas_user_id"),
        col("is_premium").alias("atlas_premium"),
        col("is_partner").alias("atlas_partner"),
        col("is_moderator").alias("atlas_moderator"),
        col("username").alias("user_name"),
    )

    # drop duplicates and missing values for Table PK
    atlas_users2_df = atlas_users2_df.dropDuplicates(["atlas_user_id"]).dropna(
        subset=["atlas_user_id"]
    )

    # The two tables need to be joined based on the user ID
    atlas_users_final_df = atlas_users1_df.join(
        atlas_users2_df, "user_name", how="left"
    )
    
    # add unique ID and write data
    atlas_users_final_df = atlas_users_final_df.withColumn(
        "user_id", monotonically_increasing_id()
    )

    # quality check and write data
    check = quality_check(
        spark, atlas_users_final_df, ["user_id", "user_name"], 11, "AtlasUsers"
    )
    assert check
    atlas_users_final_df.write.parquet(
        os.path.join(output_data, "users.parquet"),
        partitionBy=["atlas_level"],
        mode="overwrite",
    )
    
    
def process_atlas_details(spark, input_data, output_data):
    """
    Processes the Board Game Atlas board game data and writes
    the data for the AtlasDetails table to the output directory.

    Args:
        spark: spark session
        input_data: data directory
        output_data: directory to save data to

    Returns:
        None
    """

    # get filepath to read data
    atlas_games_data = os.path.join(input_data, "atlas/games/*.json")
    atlas_games_df = spark.read.json(atlas_games_data, multiLine=True)

    # only extract the columns of interest for the data model table 
    # and rename columns to match
    atlas_games_df = atlas_games_df.select(
        col("id").alias("atlas_game_id"),
        col("name").alias("game_name"),
        "url",
        "datetime_extracted",
        "year_published",
        "min_players",
        "max_players",
        "min_playtime",
        "max_playtime",
        "min_age",
        col("primary_publisher.name").alias("primary_publisher"),
        col("primary_designer.name").alias("primary_designer"),
        "artists",
        col("num_user_ratings").alias("num_ratings"),
        col("average_user_rating").alias("average_rating"),
        col("num_user_complexity_votes").alias("num_complexity"),
        "average_learning_complexity",
        "average_strategy_complexity",
    )

    # drop duplicate and missing values associated with Table PK
    atlas_games_df = atlas_games_df.dropDuplicates(["atlas_game_id"]).dropna(
        subset=["atlas_game_id"]
    )

    # convert list to string format and convert string date to timstamp
    atlas_games_df = atlas_games_df.withColumn(
        "artists", concat_ws(",", "artists")
    ).withColumn(
        "datetime_extracted",
        to_timestamp(col("datetime_extracted"), format="dd/MM/yyyy HH:mm:ss"),
    )

    # quality check and write data
    check = quality_check(
        spark, atlas_games_df, ["atlas_game_id"], 18, "AtlasDetails"
    )
    assert check
    atlas_games_df.write.parquet(
        os.path.join(output_data, "atlas_games.parquet"),
        partitionBy=["year_published"],
        mode="overwrite",
    )
    

def process_atlas_reviews(spark, input_data, output_data):
    """
    Processes the Board Game Atlas reviews data and writes
    the data for the AtlasReviews table to the output directory.
    Note that BGGDetails, AtlasDetails, and AtlasUsers
    tables must all exist and be written as Parquet files to the
    `output_data` folder prior to running this function.

    Args:
        spark: spark session
        input_data: data directory
        output_data: directory to save data to

    Returns:
        None
    """

    # get filepath and read data
    atlas_reviews_data = os.path.join(input_data, "atlas/reviews/*.json")
    atlas_reviews_df = spark.read.json(atlas_reviews_data, multiLine=True)

    # select columns of interest and rename corresponding with the data model
    atlas_reviews_df = atlas_reviews_df.select(
        col("id").alias("review_id"),
        col("createdAt").alias("review_datetime"),
        col("user_id").alias("atlas_user_id"),
        col("game_id").alias("atlas_game_id"),
        "rating",
        "description",
    )

    # drop duplicate and missing rows corresponding with table PK and date
    atlas_reviews_df = atlas_reviews_df.dropDuplicates(["review_id"]).dropna(
        subset=["review_id", "review_datetime"]
    )

    # convert string date to timestamp and add year and month columns for data partitioning for Parquet files
    atlas_reviews_df = (
        atlas_reviews_df.withColumn(
            "review_datetime",
            to_timestamp(col("review_datetime"), format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn("year", year("review_datetime"))
        .withColumn("month", month("review_datetime"))
    )

    # add the game name to the table from AtlasDetails
    atlas_games_df = spark.read.parquet(
        os.path.join(output_data, "atlas_games.parquet")
    )
    atlas_game_name = atlas_games_df.select("atlas_game_id", "game_name")
    atlas_reviews_df = atlas_reviews_df.join(
        atlas_game_name, "atlas_game_id", how="left"
    )

    # add the BoardGameGeek game ID to the table from BGGdetails
    bgg_games_df = spark.read.parquet(os.path.join(output_data, "bgg_games.parquet"))
    bgg_game_name = bgg_games_df.select("bgg_game_id", "game_name").dropDuplicates(
        ["game_name"]
    )
    atlas_reviews_df = atlas_reviews_df.join(bgg_game_name, "game_name", how="left")

    # replace the atlas_user_id with the AtlasUsers PK of user_id
    atlas_users_final_df = spark.read.parquet(
        os.path.join(output_data, "users.parquet")
    )
    atlas_user_name = atlas_users_final_df.select("user_id", "atlas_user_id")
    atlas_reviews_df = atlas_reviews_df.join(
        atlas_user_name, "atlas_user_id", how="left"
    )
    atlas_reviews_df = atlas_reviews_df.drop("atlas_user_id")

    # quality check and write data
    check = quality_check(
        spark, atlas_reviews_df, ["review_id", "review_datetime"], 10, "AtlasReviews"
    )
    assert check
    atlas_reviews_df.write.parquet(
        os.path.join(output_data, "reviews.parquet"),
        partitionBy=["year", "month"],
        mode="overwrite",
    )
    

def process_atlas_prices(spark, input_data, output_data):
    """
    Processes the Board Game Atlas prices data and writes
    the data for the AtlasPrices table to the output directory.
    Note that BGGDetails and AtlasDetails tables must all exist
    and be written as Parquet files to the `output_data` folder
    prior to running this function.

    Args:
        spark: spark session
        input_data: data directory
        output_data: directory to save data to

    Returns:
        None
    """

    # get filepath and read data
    atlas_prices_data = os.path.join(input_data, "atlas/prices/*/*.json")
    atlas_prices_df = spark.read.json(atlas_prices_data, multiLine=True)

    # extract columns of interest and rename corresponding to the data model
    atlas_prices_df = atlas_prices_df.select(
        col("id").alias("atlas_price_id"),
        col("updated_at").alias("price_datetime"),
        col("name").alias("game_name"),
        "price",
        "currency",
        "msrp",
        "url",
        "store_name",
        "country",
        "price_category",
    )

    # drop missing or duplicates rows associated with the timestamp or the table PK
    atlas_prices_df = atlas_prices_df.dropDuplicates(["atlas_price_id"]).dropna(
        subset=["atlas_price_id", "price_datetime"]
    )

    # convert string date to timestamp and add year and month columns for data partioning for Parquet files
    atlas_prices_df = (
        atlas_prices_df.withColumn(
            "price_datetime",
            to_timestamp(col("price_datetime"), format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn("year", year("price_datetime"))
        .withColumn("month", month("price_datetime"))
    )

    # add the game name to the table from AtlasDetails
    atlas_games_df = spark.read.parquet(
        os.path.join(output_data, "atlas_games.parquet")
    )
    atlas_game_name = atlas_games_df.select("atlas_game_id", "game_name")
    atlas_game_name = atlas_game_name.dropDuplicates(["game_name"])
    atlas_prices_df = atlas_prices_df.join(atlas_game_name, "game_name", how="left")

    # add the BoardGameGeek game ID to the table from BGGdetails
    bgg_games_df = spark.read.parquet(os.path.join(output_data, "bgg_games.parquet"))
    bgg_game_name = bgg_games_df.select("bgg_game_id", "game_name").dropDuplicates(
        ["game_name"]
    )
    atlas_prices_df = atlas_prices_df.join(bgg_game_name, "game_name", how="left")

    # quality check and write data
    check = quality_check(
        spark, atlas_prices_df, ["atlas_price_id", "price_datetime"], 14, "AtlasPrices"
    )
    assert check
    atlas_prices_df.write.parquet(
        os.path.join(output_data, "prices.parquet"),
        partitionBy=["year", "month"],
        mode="overwrite",
    )
    
def process_bgg_lists(spark, input_data, output_data):
    """
    Processes the BoardGameGeek lists data and writes
    the data for the BGGLists table to the output directory.
    Note that the AtlasDetails table must all exist
    and be written as Parquet files to the `output_data` folder
    prior to running this function.

    Args:
        spark: spark session
        input_data: data directory
        output_data: directory to save data to

    Returns:
        None
    """

    # get filepath and read data
    bgg_lists_data = os.path.join(input_data, "bgg/lists.csv")
    bgg_lists_df = spark.read.csv(
        bgg_lists_data, header=True, inferSchema=True, multiLine=True
    )

    # select columns and rename corresponding to the data model
    bgg_lists_df = (
        bgg_lists_df.withColumnRenamed("geeklist", "geeklist_id")
        .withColumnRenamed("game_id", "bgg_game_id")
        .withColumnRenamed("name", "game_name")
        .withColumnRenamed("user", "bgg_user_name")
        .withColumnRenamed("postdate", "post_datetime")
        .withColumnRenamed("bodytext", "description")
    )

    # convert column types
    bgg_lists_df = (
        bgg_lists_df.withColumn(
            "post_datetime",
            to_timestamp(
                col("post_datetime"), format="EEE, dd MMM yyyy HH:mm:ss +SSSS"
            ),
        )
        .withColumn("geeklist_id", col("geeklist_id").cast(IntegerType()))
        .withColumn("bgg_game_id", col("bgg_game_id").cast(IntegerType()))
    )

    # add UID to Table and drop any missing geeklist_id or timestamp
    bgg_lists_df = bgg_lists_df.withColumn(
        "list_item_id", monotonically_increasing_id()
    ).dropna(subset=["geeklist_id", "post_datetime"])

    # add year and month columns for data partitioning for Parquet files
    bgg_lists_df = bgg_lists_df.withColumn("year", year("post_datetime")).withColumn(
        "month", month("post_datetime")
    )

    # add the Atlas game ID to the table from AtlasDetails
    atlas_games_df = spark.read.parquet(
        os.path.join(output_data, "atlas_games.parquet")
    )
    atlas_game_name = atlas_games_df.select("atlas_game_id", "game_name")
    atlas_game_name = atlas_game_name.dropDuplicates(["game_name"])
    bgg_lists_df = bgg_lists_df.join(atlas_game_name, "game_name", how="left")

    # quality check and write data
    check = quality_check(
        spark, bgg_lists_df, ["list_item_id", "post_datetime"], 11, "BGGLists"
    )
    assert check
    bgg_lists_df.write.parquet(
        os.path.join(output_data, "lists.parquet"),
        partitionBy=["year", "month"],
        mode="overwrite",
    )
    
    
def process_time(spark, input_data, output_data):
    """
    Creates the Time table and write to the output directory.
    Note that the AtlasReviews, AtlasPrices, and BGGLists
    tables must all exist and be written as Parquet files 
    to the `output_data` folder prior to running this function.

    Args:
        spark: spark session
        input_data: data directory
        output_data: directory to save data to

    Returns:
        None
    """
    
    # read all required data
    atlas_reviews_df = spark.read.parquet(os.path.join(output_data, "reviews.parquet"))
    atlas_prices_df = spark.read.parquet(os.path.join(output_data, "prices.parquet"))
    bgg_lists_df = spark.read.parquet(os.path.join(output_data, "lists.parquet"))

    # collect all datetimes
    all_datetime_df = (
        atlas_reviews_df.select(col("review_datetime").alias("datetime"))
        .union(atlas_prices_df.select(col("price_datetime").alias("datetime")))
        .union(bgg_lists_df.select(col("post_datetime").alias("datetime")))
        .dropDuplicates(["datetime"])
    )

    # extract columns of interest matching the data model
    time_df = all_datetime_df.select(
        "datetime",
        minute("datetime").alias("minute"),
        hour("datetime").alias("hour"),
        dayofmonth("datetime").alias("day"),
        weekofyear("datetime").alias("week"),
        month("datetime").alias("month"),
        year("datetime").alias("year"),
        dayofweek("datetime").alias("weekday"),
    )

    # quality check and write data
    check = quality_check(
        spark, time_df, ["datetime"], 8, "Time"
    )
    assert check
    time_df.write.parquet(
        os.path.join(output_data, "time.parquet"),
        partitionBy=["year", "month"],
        mode="overwrite",
    )

    
def main():
    """
    Creates a spark session and processes
    all Tables.
    """
    spark = create_spark_session()
    
    # uncomment next 2 lines to use local (update paths as req)
    input_data = "data/raw"
    output_data = "data/output"
    
    # uncomment next 2 lines to use S3 (update paths as req)
    # note that `upload_s3.py` will transfer local data to S3
    # input_data = "s3a://data-model-test-project/raw"
    # output_data = "s3a://data-model-test-project/output"
    
    process_bgg_details(spark, input_data, output_data)    
    process_atlas_users(spark, input_data, output_data) 
    process_atlas_details(spark, input_data, output_data) 
    process_atlas_reviews(spark, input_data, output_data) 
    process_atlas_prices(spark, input_data, output_data) 
    process_bgg_lists(spark, input_data, output_data) 
    process_time(spark, input_data, output_data) 


if __name__ == "__main__":
    main()
