from pyspark.sql import SparkSession, DataFrame
from pyspark import RDD
from pyspark.sql.functions import col, regexp_extract


def _extract_attribute(attribute: str, regex: str, df: DataFrame) -> DataFrame:
    return df.withColumn(
            attribute, 
            regexp_extract(
                col("content"),
                regex,
                1,
            )
        )


def parse_crawled_pages(
        spark: SparkSession, 
        crawled_pages_path: str,
    ) -> DataFrame:
    rdd: RDD[tuple[str, str]] = spark.sparkContext.wholeTextFiles(
        crawled_pages_path,
        minPartitions=400
    )
    pages_dataframe: DataFrame = rdd.toDF(["filename", "content"])
    pages_dataframe = pages_dataframe.filter(col("filename").contains("com_drivers"))

    pages_dataframe = _extract_attribute(
        "name",
        r"<title>Driver: (.*) \| Driver Database</title>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "nationality",
        r"<div>nationality</div>\n*?\s*?<div>([^<]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "series",
        r"</div>\n*?\s*?</div>\n*?\s*?<div class=\"[\w ]*?\">([\w -]+)</div>\n*\s*</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "age",
        r"<div>age</div>\n*?\s*?<div>([^<]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "birthday",
        r"<div>birthday</div>\n*?\s*?<div>([^<]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "hometown",
        r"<div>hometown</div>\n*?\s*?<div>([^<]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "races_started",
        r">Driver Stats</h2>[\s\S]*?races started</div>\n?[ \t]*<div>([0-9]*)</div>",
        pages_dataframe
    )
    
    pages_dataframe = _extract_attribute(
        "races_entered",
        r">Driver Stats</h2>[\s\S]*?races entered</div>\n?[ \t]*<div>([0-9]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "wins",
        r">Driver Stats</h2>[\s\S]*?wins</div>\n?[ \t]*<div>([0-9]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "podiums",
        r">Driver Stats</h2>[\s\S]*?podiums</div>\n?[ \t]*<div>([0-9]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "pole_positions",
        r">Driver Stats</h2>[\s\S]*?pole positions</div>\n?[ \t]*<div>([0-9]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "fastest_laps",
        r">Driver Stats</h2>[\s\S]*?fastest laps</div>\n?[ \t]*<div>([0-9]*)</div>",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "race_win_percentage",
        r">Driver Stats</h2>[\s\S]*?race win percentage</div>\n?[ \t]*<div>([0-9.]*)(?:<!-- -->)*%",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "podium_percentage",
        r">Driver Stats</h2>[\s\S]*?podium percentage</div>\n?[ \t]*<div>([0-9.]*)(?:<!-- -->)*%",
        pages_dataframe
    )

    pages_dataframe = _extract_attribute(
        "driverdb_score",
        r"driverdb score</div>\n?[ \t]*<div>([0-9,]*)",
        pages_dataframe
    )
    
    return pages_dataframe