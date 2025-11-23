import logging
import os
import shutil
from dataclasses import dataclass
from dotenv import load_dotenv
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_extract, from_xml, broadcast
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, LongType

from lib.html_parser import parse_crawled_pages
from lib.pylucene import search_index, create_index

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
    parsed_pages_tsv_path: str
    complete_data_tsv_path: str
    index_path: str


def load_configuration() -> ProjectConfiguration:
    logging.debug("Staring loading of the configuration")
    load_dotenv()
    wiki_path = os.getenv("WIKI_DUMP_PATH")
    pages_path = os.getenv("CRAWLED_PAGES_PATH")
    parsed_pages_path = os.getenv("PARSED_PAGES_TSV_PATH")
    complete_data_path = os.getenv("COMPLETE_DATA_TSV_PATH")
    index_path = os.getenv("INDEX_PATH")

    # Error checks for the loaded values
    if (wiki_path is None):
        logging.error("Path to wiki dump not found")
        raise AttributeError("Path to wiki dump not found")
    if (pages_path is None):
        logging.error("Path to crawled pages not found")
        raise AttributeError("Path to crawled paged not found")
    if (parsed_pages_path is None):
        logging.error("Path to the parsed pages tsv not found")
        raise AttributeError("Path to the parsed pages tsv not found")
    if (complete_data_path is None):
        logging.error("Path to the complete data tsv not found")
        raise AttributeError("Path to the complete data tsv not found")
    if (index_path is None):
        logging.error("Path to the index not found")
        raise AttributeError("Path to the index not found")
    return ProjectConfiguration(
        wiki_dump_path=wiki_path,
        crawled_pages_path=pages_path,
        parsed_pages_tsv_path=parsed_pages_path,
        complete_data_tsv_path=complete_data_path,
        index_path=index_path
    )


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


def load_wiki_dump() -> DataFrame:
    pass


def check_if_file_exist(path: str) -> bool:
    if not os.path.exists(path):
        return False
    return os.path.isfile(path)


def save_dataframe_to_tsv(dataframe: DataFrame, path_str: str) -> None:
    path = Path(path_str)
    tmp_path = path.parent / "tmp"
    dataframe.coalesce(1).write.csv(
        path=str(tmp_path),
        sep="\t",
        header=True,
        mode="overwrite"
    )
    for file in tmp_path.iterdir():
        if file.name.startswith("part-"):
            shutil.move(str(file), str(path))
            break
    shutil.rmtree(str(tmp_path))


def main() -> None:
    logging.info("Started")
    try:
        configuration = load_configuration()
    except AttributeError:
        logging.error("Configuration failed to load")
        exit()
    logging.info("Configuration loaded")

    while True:
        print("Select an operation:")
        print("\t(1) Parse data")
        print("\t(2) Index data")
        print("\t(3) Search")
        print("\t(4) Quit")

        try:
            selected_mode = int(input("Selected operation: "))
        except ValueError:
            print("Please insert a number!")
            continue

        if selected_mode in [1, 2, 3, 4]:
            break
        else:
            print("Invalid number, please insert a valid number.")
    
    if selected_mode == 3:
        logging.info("Selected mode: Search")
        query = input("Search query: ")
        search_index(configuration.index_path, query)
        exit()
    elif selected_mode == 4:
        exit()
    
    # Create spark session
    spark = create_spark_session()
    logging.info("Spark session created")

    logging.info("Checking if re-parse is needed")
    if check_if_file_exist(configuration.parsed_pages_tsv_path):
        logging.info("Existing data found, skipping reparsing")
        driverdb_dataframe = spark.read.csv(
            configuration.parsed_pages_tsv_path,
            sep="\t",
            header=True,
            inferSchema=True,
        )
    else:
        # Try to load the data from html pages
        logging.info("Starting to parse the crawled pages")
        driverdb_dataframe: DataFrame = parse_crawled_pages(spark, configuration.crawled_pages_path)
        logging.info("Dropping the 'content' field")
        driverdb_dataframe = driverdb_dataframe.drop("content")
        logging.info("Repartitioning the data")
        driverdb_dataframe.repartition(500)
        logging.info("Saving the dataframe")
        save_dataframe_to_tsv(
            driverdb_dataframe, 
            configuration.parsed_pages_tsv_path,
        )

    # Try to display the processed data
    logging.info("Displaying loaded data")
    driverdb_dataframe.show()

    logging.info("Starting the processing of wikipedia dump")
    rdd = spark.sparkContext.textFile(configuration.wiki_dump_path)
    start_tag = "<page>"
    end_tag = "</page>"
    def split_pages(lines):
        buf = []
        in_page = False
        for line in lines:
            if start_tag in line:
                in_page = True
                buf = [line]
            elif in_page:
                buf.append(line)
            if in_page and end_tag in line:
                yield "\n".join(buf)
                buf = []
                in_page = False

    pages = rdd.mapPartitions(split_pages)
    df_raw = spark.createDataFrame(pages.map(lambda x: (x,)), ["raw_xml"])
    df_parsed = df_raw.select(from_xml(col("raw_xml"), WIKI_SCHEMA).alias("page")).select("page.*")
    

    out = df_parsed.select(
        col("title").alias("title"),
        col("revision.text._VALUE").alias("text"),
    )

    series = driverdb_dataframe.select("series").filter(col("series") != "").distinct()
    series_info = out.join(
        broadcast(series), out.title == series.series
    ).select(out["*"])

    series_info = series_info.withColumn(
        "series_description",
        regexp_extract(col("text")[0], r"('''.*?)\n", 1)
    )

    series_info.show()

    logging.info(f"Connected series pages: {series_info.distinct().count()}")

    merged_dataframe = driverdb_dataframe.join(series_info, driverdb_dataframe["series"] == series_info["title"], "left")
    merged_dataframe = merged_dataframe.fillna("")
    merged_dataframe.show()

    driver_names = driverdb_dataframe.select("name").filter(col("name") != "").distinct()
    driver_info = out.join(
        broadcast(driver_names), out.title == driver_names.name
    ).select(out["*"])
    driver_info = driver_info.drop_duplicates()
    driver_info = driver_info.filter(col("text")[0].rlike("(?i)racing driver"))
    driver_info = driver_info.withColumn(
        "current_team",
        regexp_extract(col("text")[0], r"(?:t|T)eam\s*=\s*\[\[(.*?)]]", 1)
    )
    driver_info = driver_info.withColumn(
        "all_teams",
        regexp_extract(col("text")[0], r"(?:t|T)eam\(*s\)*\s*=\s*(.*?)\n", 1)
    )
    driver_info = driver_info.withColumn(
        "car_number",
        regexp_extract(col("text")[0], r"car(?:_| )number\s*=\s*(\d+)", 1)
    )
    driver_info = driver_info.withColumn(
        "championships",
        regexp_extract(col("text")[0], r"(?:c|C)hampionships\s*=\s*(\d+)", 1)
    )
    driver_info = driver_info.withColumn(
        "bio",
        regexp_extract(col("text")[0], r"('''.*?)\n", 1)
    )
    driver_info = driver_info.drop("text")


    logging.info(f"Connected driver pages: {driver_info.distinct().count()}")
    driver_info.show(50)

    merged_dataframe = merged_dataframe.join(driver_info, merged_dataframe["name"] == driver_info["title"], "left")
    merged_dataframe = merged_dataframe.fillna("")
    merged_dataframe.show()

    logging.info("Attempting to get information about teams.")
    team_names = driver_info.select("current_team").filter(col("current_team") != "").distinct()
    team_info = out.join(
        broadcast(team_names), out.title == team_names.current_team
    ).select(out["*"])

    team_info = team_info.withColumn(
        "team_description",
        regexp_extract(col("text")[0], r"('''.*?)\n", 1)
    )

    logging.info(f"Connected team pages: {team_info.distinct().count()}")

    logging.info("Showing gathered team information:")
    team_info.show()

    merged_dataframe = merged_dataframe.join(team_info, merged_dataframe["series"] == team_info["title"], "left")
    merged_dataframe = merged_dataframe.fillna("")
    merged_dataframe = merged_dataframe.drop("text")
    merged_dataframe = merged_dataframe.drop("title")
    merged_dataframe.show()

    save_dataframe_to_tsv(merged_dataframe, configuration.complete_data_tsv_path)
    create_index(configuration.complete_data_tsv_path, configuration.index_path)

if __name__ == "__main__":
    main()