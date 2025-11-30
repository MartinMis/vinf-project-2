import logging
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, from_xml, regexp_extract, when


from lib.html_parser import parse_crawled_pages
from lib.pylucene import create_index, search_index
from lib.utills import ProjectConfiguration, WIKI_SCHEMA
from lib.user_interface import print_header, print_mode_menu, print_results_list

logging.basicConfig(level=logging.INFO)


def load_configuration() -> ProjectConfiguration:
    logging.debug("Staring loading of the configuration")
    load_dotenv()
    wiki_path = os.getenv("WIKI_DUMP_PATH")
    pages_path = os.getenv("CRAWLED_PAGES_PATH")
    parsed_pages_path = os.getenv("PARSED_PAGES_TSV_PATH")
    complete_data_path = os.getenv("COMPLETE_DATA_TSV_PATH")
    index_path = os.getenv("INDEX_PATH")

    # Error checks for the loaded values
    if wiki_path is None:
        logging.error("Path to wiki dump not found")
        raise AttributeError("Path to wiki dump not found")
    if pages_path is None:
        logging.error("Path to crawled pages not found")
        raise AttributeError("Path to crawled paged not found")
    if parsed_pages_path is None:
        logging.error("Path to the parsed pages tsv not found")
        raise AttributeError("Path to the parsed pages tsv not found")
    if complete_data_path is None:
        logging.error("Path to the complete data tsv not found")
        raise AttributeError("Path to the complete data tsv not found")
    if index_path is None:
        logging.error("Path to the index not found")
        raise AttributeError("Path to the index not found")
    return ProjectConfiguration(
        wiki_dump_path=wiki_path,
        crawled_pages_path=pages_path,
        parsed_pages_tsv_path=parsed_pages_path,
        complete_data_tsv_path=complete_data_path,
        index_path=index_path,
    )


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SparkParser")
        .master("local[4]")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.instances", "4")
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "4g")
        .config("spark.default.parallelism", "400")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def load_wiki_dump(spark, configuration: ProjectConfiguration) -> DataFrame:
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
    return df_raw.select(from_xml(col("raw_xml"), WIKI_SCHEMA).alias("page")).select(
        "page.*"
    )


def check_if_file_exist(path: str) -> bool:
    if not os.path.exists(path):
        return False
    return os.path.isfile(path)


def save_dataframe_to_tsv(dataframe: DataFrame, path_str: str) -> None:
    path = Path(path_str)
    tmp_path = path.parent / "tmp"
    dataframe.coalesce(1).write.csv(
        path=str(tmp_path), sep="\t", header=True, mode="overwrite"
    )
    for file in tmp_path.iterdir():
        if file.name.startswith("part-"):
            shutil.move(str(file), str(path))
            break
    shutil.rmtree(str(tmp_path))


def parse_wiki(configuration: ProjectConfiguration):
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
        driverdb_dataframe: DataFrame = parse_crawled_pages(
            spark, configuration.crawled_pages_path
        )
        logging.info("Dropping the 'content' field")
        driverdb_dataframe = driverdb_dataframe.drop("content")
        logging.info("Repartitioning the data")
        driverdb_dataframe = driverdb_dataframe.repartition(100)
        logging.info("Saving the dataframe")
        save_dataframe_to_tsv(
            driverdb_dataframe,
            configuration.parsed_pages_tsv_path,
        )

    df_parsed = load_wiki_dump(spark, configuration)
    df_parsed = solve_redirects(df_parsed)
    df_parsed.persist()

    out = df_parsed.select(
        col("title"),
        col("content").alias("text"),
    )
    out.persist()

    series = driverdb_dataframe.select("series").filter(col("series") != "").distinct()
    series_info = out.join(broadcast(series), out.title == series.series).select(
        out["*"]
    )

    series_info = series_info.withColumn(
        "series_description", regexp_extract(col("text")[0], r"('''.*?)\n", 1)
    )

    logging.info(f"Connected series pages: {series_info.distinct().count()}")

    merged_dataframe = driverdb_dataframe.join(
        series_info, driverdb_dataframe["series"] == series_info["title"], "left"
    )
    merged_dataframe = merged_dataframe.fillna("")
    merged_dataframe.show()

    driver_names = (
        driverdb_dataframe.select("name").filter(col("name") != "").distinct()
    )
    driver_info = out.join(
        broadcast(driver_names), out.title == driver_names.name
    ).select(out["*"])
    driver_info = driver_info.drop_duplicates()
    driver_info = driver_info.filter(col("text")[0].rlike("(?i)racing driver"))
    driver_info = driver_info.withColumn(
        "current_team",
        regexp_extract(col("text")[0], r"(?:t|T)eam\s*=\s*\[\[(.*?)]]", 1),
    )
    driver_info = driver_info.withColumn(
        "all_teams",
        regexp_extract(col("text")[0], r"(?:t|T)eam\(*s\)*\s*=\s*(.*?)\n", 1),
    )
    driver_info = driver_info.withColumn(
        "car_number", regexp_extract(col("text")[0], r"car(?:_| )number\s*=\s*(\d+)", 1)
    )
    driver_info = driver_info.withColumn(
        "championships",
        regexp_extract(col("text")[0], r"(?:c|C)hampionships\s*=\s*(\d+)", 1),
    )
    driver_info = driver_info.withColumn(
        "bio", regexp_extract(col("text")[0], r"('''.*?)\n", 1)
    )
    driver_info = driver_info.drop("text")

    logging.info(f"Connected driver pages: {driver_info.distinct().count()}")

    merged_dataframe = merged_dataframe.join(
        driver_info, merged_dataframe["name"] == driver_info["title"], "left"
    )
    merged_dataframe = merged_dataframe.fillna("")

    logging.info("Attempting to get information about teams.")
    team_names = (
        driver_info.select("current_team").filter(col("current_team") != "").distinct()
    )
    team_info = out.join(
        broadcast(team_names), out.title == team_names.current_team
    ).select(out["*"])

    team_info = team_info.withColumn(
        "team_description", regexp_extract(col("text")[0], r"('''.*?)\n", 1)
    )

    logging.info(f"Connected team pages: {team_info.distinct().count()}")

    logging.info("Showing gathered team information:")

    merged_dataframe = merged_dataframe.join(
        team_info, merged_dataframe["current_team"] == team_info["title"], "left"
    )
    merged_dataframe = merged_dataframe.fillna("")
    merged_dataframe = merged_dataframe.drop("text")
    merged_dataframe = merged_dataframe.drop("title")

    save_dataframe_to_tsv(merged_dataframe, configuration.complete_data_tsv_path)


def solve_redirects(loaded_wiki_data: DataFrame):
    # Get the pages with redirects
    pages_with_redirect = loaded_wiki_data.filter(col("redirect").isNotNull()).select(
        col("title").alias("redirect_source"),
        col("redirect._title").alias("redirect_target"),
    )
    # Get the pages without redirects
    pages_without_redirect = loaded_wiki_data.filter(col("redirect").isNull()).select(
        col("title"), col("revision.text._VALUE").alias("text")
    )
    current = pages_with_redirect

    logging.info("Attempting to solve_redirects")
    for _ in range(1):
        next_iteration = (
            current.alias("c")
            .join(
                pages_with_redirect.alias("r"),
                col("c.redirect_target") == col("r.redirect_source"),
                how="left",
            )
            .select(
                col("c.redirect_source"),
                when(col("r.redirect_target").isNotNull(), col("r.redirect_target"))
                .otherwise(col("c.redirect_source"))
                .alias("redirect_target"),
            )
        )

        if (
            current.join(
                next_iteration, ["redirect_source", "redirect_target"], "inner"
            ).count()
            == current.count()
        ):
            break

        current = next_iteration

    resolved_redirects = current
    resolved = (
        resolved_redirects.alias("r")
        .join(
            pages_without_redirect.alias("a"),
            col("r.redirect_target") == col("a.title"),
            how="left",
        )
        .select(
            col("r.redirect_source").alias("title"),
            col("r.redirect_target").alias("article_title"),
            col("a.text").alias("content"),
        )
    )
    real = pages_without_redirect.select(
        col("title"), col("title").alias("article_title"), col("text").alias("content")
    )
    return real.unionByName(resolved)


def main() -> None:
    logging.info("Started")
    try:
        configuration = load_configuration()
    except AttributeError:
        logging.error("Configuration failed to load")
        exit()
    logging.info("Configuration loaded")

    print_header()
    selected_mode = print_mode_menu()

    if selected_mode == "3":
        logging.info("Selected mode: Search")
        query = input("Search query: ")
        search_index(configuration.index_path, query)
        exit()
    elif selected_mode == 4:
        exit()

    parse_wiki(configuration)
    create_index(configuration.complete_data_tsv_path, configuration.index_path)


if __name__ == "__main__":
    main()
