import logging
import os
from dataclasses import dataclass
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_extract, from_xml, broadcast
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, LongType

from lib.html_parser import parse_crawled_pages

logging.basicConfig(level=logging.INFO)


WIKI_SCHEMA = StructType([
        StructField("title", StringType(), True),
        StructField("ns", IntegerType(), True),
        StructField("id", LongType(), True),
        StructField("redirect", StructType([
            StructField("_title", StringType(), True)
        ]), True),
        StructField("revision", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("timestamp", StringType(), True),
            StructField("contributor", StructType([
                StructField("username", StringType(), True),
                StructField("ip", StringType(), True)
            ]), True),
            StructField("text", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_bytes", LongType(), True)
            ]), True)
        ])), True)
    ])


@dataclass
class ProjectConfiguration:
    wiki_dump_path: str
    crawled_pages_path: str


def load_configuration() -> ProjectConfiguration:
    logging.debug("Staring loading of the configuration")
    load_dotenv()
    wiki_path = os.getenv("WIKI_DUMP_PATH")
    pages_path = os.getenv("CRAWLED_PAGES_PATH")

    # Error checks for the loaded values
    if (wiki_path is None):
        logging.error("Path to wiki dump not found")
        raise AttributeError("Path to wiki dump not found")
    if (pages_path is None):
        logging.error("Path to crawled pages not found")
        raise AttributeError("Path to crawled paged not found")
    
    return ProjectConfiguration(wiki_dump_path=wiki_path, crawled_pages_path=pages_path)


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("SparkParser") \
        .master("local[*]") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "4g") \
        .config("spark.default.parallelism", "400") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()


def main() -> None:
    logging.info("Started")
    try:
        configuration = load_configuration()
    except AttributeError:
        logging.error("Configuration failed to load")
        exit()
    logging.info("Configuration loaded")
    
    # Create spark session
    spark = create_spark_session()
    logging.info("Spark session created")

    # Try to load the data from html pages
    logging.info("Starting to parse the crawled pages")
    driverdb_dataframe: DataFrame = parse_crawled_pages(spark, configuration.crawled_pages_path)
    logging.info("Dropping the 'filename' field")
    driverdb_dataframe = driverdb_dataframe.drop("filename")
    logging.info("Dropping the 'content' field")
    driverdb_dataframe = driverdb_dataframe.drop("content")

    # Try to display the processed data
    logging.info("Displaying loaded data")
    driverdb_dataframe.show()


if __name__ == "__main__":
    main()