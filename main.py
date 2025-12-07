import logging
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, from_xml, regexp_extract, when
from pyspark import StorageLevel

from lib.html_parser import parse_crawled_pages
from lib.pylucene import create_index, search_index
from lib.utills import ProjectConfiguration, WIKI_SCHEMA, get_logger
from lib.user_interface import print_header, print_mode_menu, print_results_list

log = get_logger("main")


def load_configuration() -> ProjectConfiguration:
    log.debug("Staring loading of the configuration")
    load_dotenv()
    wiki_path = os.getenv("WIKI_DUMP_PATH")
    pages_path = os.getenv("CRAWLED_PAGES_PATH")
    parsed_pages_path = os.getenv("PARSED_PAGES_TSV_PATH")
    complete_data_path = os.getenv("COMPLETE_DATA_TSV_PATH")
    index_path = os.getenv("INDEX_PATH")

    # Error checks for the loaded values
    if wiki_path is None:
        log.error("Path to wiki dump not found")
        raise AttributeError("Path to wiki dump not found")
    if pages_path is None:
        log.error("Path to crawled pages not found")
        raise AttributeError("Path to crawled paged not found")
    if parsed_pages_path is None:
        log.error("Path to the parsed pages tsv not found")
        raise AttributeError("Path to the parsed pages tsv not found")
    if complete_data_path is None:
        log.error("Path to the complete data tsv not found")
        raise AttributeError("Path to the complete data tsv not found")
    if index_path is None:
        log.error("Path to the index not found")
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
        .master("local[*]")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.memory", "2g")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def load_wiki_dump(spark, configuration: ProjectConfiguration) -> DataFrame:
    log.info("Starting the processing of wikipedia dump")
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
    log.info("Spark session created")

    log.info("Checking if re-parse is needed")
    if check_if_file_exist(configuration.parsed_pages_tsv_path):
        log.info("Existing data found, skipping reparsing")
        driverdb_dataframe = spark.read.csv(
            configuration.parsed_pages_tsv_path,
            sep="\t",
            header=True,
            inferSchema=True,
        )
    else:
        # Try to load the data from html pages
        log.info("Starting to parse the crawled pages")
        driverdb_dataframe: DataFrame = parse_crawled_pages(
            spark, configuration.crawled_pages_path
        )
        log.info("Dropping the 'content' field")
        driverdb_dataframe = driverdb_dataframe.drop("content")
        log.info("Repartitioning the data")
        driverdb_dataframe = driverdb_dataframe.repartition(100)
        log.info("Saving the dataframe")
        save_dataframe_to_tsv(
            driverdb_dataframe,
            configuration.parsed_pages_tsv_path,
        )

    df_parsed = load_wiki_dump(spark, configuration)
    df_parsed = solve_redirects(df_parsed)
    logging.info("Preparing to check")

    out = df_parsed.select(
        col("title"),
        col("content")[0].alias("text"),
    )
    out = out.repartition(200)

    series = driverdb_dataframe.select(
        "series").filter(col("series") != "").distinct()
    series_info = out.join(series, out.title == series.series).select(
        col("title"),
        regexp_extract(
            col("text"), r"(^'''[^\[].*?)\n", 1).alias("series_description"),
    )

    merged_dataframe = driverdb_dataframe.join(
        broadcast(series_info),
        driverdb_dataframe["series"] == series_info["title"],
        "left",
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
    driver_info = driver_info.filter(col("text").rlike("(?i)racing driver"))
    driver_info = driver_info.select(
        col("*"),
        regexp_extract(
            col("text"), r"(?:t|T)eam\s*=\s*\[{0,2}(.*?)(?:\n|<|]]|\|)", 1
        ).alias("current_team"),
        regexp_extract(col("text"), r"(?:t|T)eam\(*s\)*\s*=\ *(.*)\n", 1).alias(
            "all_teams"
        ),
        regexp_extract(col("text"), r"car(?:_| )number\s*=\s*(\d+)", 1).alias(
            "car_number"
        ),
        regexp_extract(col("text"), r"(?:c|C)hampionships\s*=\s*(\d+)", 1).alias(
            "championships"
        ),
        regexp_extract(col("text"), r"('''.*?)\n", 1).alias("bio"),
    )
    driver_info = driver_info.drop("text")

    merged_dataframe = merged_dataframe.join(
        broadcast(
            driver_info), merged_dataframe["name"] == driver_info["title"], "left"
    )
    merged_dataframe = merged_dataframe.fillna("")

    log.info("Attempting to get information about teams.")
    team_names = (
        driver_info.select("current_team").filter(
            col("current_team") != "").distinct()
    )
    team_info = out.join(
        broadcast(team_names), out.title == team_names.current_team
    ).select(out["*"])

    team_info = team_info.withColumn(
        "team_description", regexp_extract(col("text"), r"(^'''[^\[].*?)\n", 1)
    )

    merged_dataframe = merged_dataframe.join(
        broadcast(team_info),
        merged_dataframe["current_team"] == team_info["title"],
        "left",
    )
    merged_dataframe = merged_dataframe.fillna("")
    merged_dataframe = merged_dataframe.drop("text")
    merged_dataframe = merged_dataframe.drop("title")

    save_dataframe_to_tsv(
        merged_dataframe, configuration.complete_data_tsv_path)


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

    log.info("Attempting to solve_redirects")
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
                when(col("r.redirect_target").isNotNull(),
                     col("r.redirect_target"))
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
        col("title"), col("title").alias(
            "article_title"), col("text").alias("content")
    )
    return real.unionByName(resolved)


def main() -> None:
    log.info("Started")
    try:
        configuration = load_configuration()
    except AttributeError:
        log.error("Configuration failed to load")
        exit()
    log.info("Configuration loaded")

    print_header()
    selected_mode = print_mode_menu()

    match selected_mode:
        case "1":
            parse_wiki(configuration)
        case "2":
            create_index(
                configuration.index_path,
                configuration.complete_data_tsv_path,
            )
        case "3":
            while True:
                query = input("Search query: ")
                if query.strip() == "@exit":
                    break
                search_results = search_index(
                    configuration.index_path,
                    query,
                )
                print_results_list(search_results)


if __name__ == "__main__":
    main()
