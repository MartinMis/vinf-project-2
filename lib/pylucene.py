import lucene
import logging
import csv
import os
import re
from pathlib import Path
from java.io import File
from java.lang import Integer, Long, Double, String
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import (
    Document,
    Field,
    TextField,
    StringField,
    IntPoint,
    StoredField,
    DoublePoint,
)
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
from org.apache.lucene.search import (
    IndexSearcher,
    BooleanQuery,
    BooleanClause,
    BoostQuery,
)
from org.apache.lucene.index import DirectoryReader
from java.util import HashMap
from dataclasses import dataclass


@dataclass
class FieldsAndWeights:
    content: float = 1.0


logger = logging.getLogger("pylucene")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def remove_tags(html: str) -> str:
    edited_html = re.sub(r"<script[^>]*>[.\n\s\S]*?</script[^>]*>", " ", html)
    return re.sub(r"<.*?>", " ", edited_html)


def extract_content_from_html(file_path_str: str) -> str:
    file_path = Path(file_path_str)
    if not file_path.is_file():
        logger.error("Invalid file path!")
        raise AttributeError("Invalid file path!")

    with file_path.open("r", encoding="utf-8") as html_file:
        content = html_file.read()
        return remove_tags(content)


def parse_query_args(query: str):
    argument_pattern = r"@(\w+)=(\w+)"
    max_num_pattern = r"\$(\w+)=(\w+)"
    min_num_pattern = r"\^(\w+)=(\w+)"
    arguments = re.findall(argument_pattern, query)
    max_values = re.findall(max_num_pattern, query)
    min_values = re.findall(min_num_pattern, query)
    text = re.sub(argument_pattern, "", query).strip()
    text = re.sub(max_num_pattern, "", text).strip()
    text = re.sub(min_num_pattern, "", text).strip()

    arguments = [(name, value.replace("_", " ")) for name, value in arguments]

    return text, arguments, min_values, max_values


def create_index(index_str_path: str, data_tsv_path: str):
    logger.info("Initializing the lucene VM")
    lucene.initVM()

    logger.info("Setting up the index writter")
    analyzer = StandardAnalyzer()
    index_path = File(index_str_path).toPath()
    index_store = FSDirectory.open(index_path)

    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(index_store, config)

    data_tsv = Path(data_tsv_path)
    try:
        with data_tsv.open("r", encoding="utf-8") as tsv_file:
            tsv_reader = csv.DictReader(tsv_file, delimiter="\t")

            for row in tsv_reader:
                filename = row.get("filename", "")
                driver_name = row.get("name", "")
                nationality = row.get("nationality", "")
                series = row.get("series", "")
                age = row.get("age", "0")
                birthday = row.get("birthday", "")
                hometown = row.get("hometown", "")
                races_started = row.get("races_started", "0")
                races_entered = row.get("races_entered", "0")
                wins = row.get("wins", "0")
                podiums = row.get("podiums", "0")
                pole_positions = row.get("pole_positions", "0")
                fastest_laps = row.get("fastest_laps", "0")
                race_win_percentage = row.get("race_win_percentage", "0.0")
                podium_percentage = row.get("podium_percentage", "0.0")
                driverdb_score = row.get("driverdb_score", "0")
                current_team = row.get("current_team", "")
                all_teams = row.get("all_teams", "")
                car_number = row.get("car_number", "")
                championships = row.get("championships", "0")
                driver_bio = row.get("bio", "")
                series_description = row.get("series_description", "")
                team_description = row.get("team_description", "")

                age = 0 if age == "" else int(age)
                races_started = 0 if races_started == "" else int(races_started)
                races_entered = 0 if races_entered == "" else int(races_entered)
                wins = 0 if wins == "" else int(wins)
                podiums = 0 if podiums == "" else int(podiums)
                pole_positions = 0 if pole_positions == "" else int(pole_positions)
                fastest_laps = 0 if fastest_laps == "" else int(fastest_laps)
                race_win_percentage = (
                    0.0 if race_win_percentage == "" else float(race_win_percentage)
                )
                podium_percentage = (
                    0.0 if podium_percentage == "" else float(podium_percentage)
                )
                championships = 0 if championships == "" else int(championships)

                document = Document()

                if filename != "":
                    logger.debug("Extracting HTML content for the index.")
                    file_path_str = filename.split(":")[1]
                    document.add(
                        TextField(
                            "content",
                            extract_content_from_html(file_path_str),
                            Field.Store.NO,
                        )
                    )
                else:
                    logger.warning(
                        f"No related HTML content found for driver {driver_name}!"
                    )

                logger.debug("Creating new document.")

                # drivers name
                document.add(TextField("driver_name", driver_name, Field.Store.YES))
                # nationality
                document.add(TextField("nationality", nationality, Field.Store.YES))
                # series
                document.add(TextField("series", series, Field.Store.YES))
                # age
                document.add(IntPoint("age", age))
                document.add(StoredField("age", age))
                # birthday
                # TODO Make this work like a number
                document.add(TextField("birthday", birthday, Field.Store.YES))
                # hometown
                document.add(TextField("hometown", hometown, Field.Store.YES))
                # races started
                document.add(IntPoint("races_started", races_started))
                document.add(StoredField("races_started", races_started))
                # races entered
                document.add(IntPoint("races_entered", races_entered))
                document.add(StoredField("races_entered", races_entered))
                # wins
                document.add(IntPoint("wins", wins))
                document.add(StoredField("wins", wins))
                # podiums
                document.add(IntPoint("podiums", podiums))
                document.add(StoredField("podiums", podiums))
                # pole positions
                document.add(IntPoint("pole_positions", pole_positions))
                document.add(StoredField("pole_positions", pole_positions))
                # fastest laps
                document.add(IntPoint("fastest_laps", fastest_laps))
                document.add(StoredField("fastest_laps", fastest_laps))
                # race win percentage
                document.add(DoublePoint("race_win_percentage", race_win_percentage))
                document.add(StoredField("race_win_percentage", race_win_percentage))
                # podium percentage
                document.add(DoublePoint("podium_percentage", podium_percentage))
                document.add(StoredField("podium_percentage", podium_percentage))
                # driverdb score
                # TODO: Make this a number
                document.add(
                    TextField("driverdb_score", driverdb_score, Field.Store.YES)
                )
                # current team
                document.add(TextField("current_team", current_team, Field.Store.YES))
                # all teams
                document.add(TextField("all_teams", all_teams, Field.Store.YES))
                # car number
                document.add(TextField("car_number", car_number, Field.Store.YES))
                # championships
                document.add(IntPoint("championships", championships))
                document.add(StoredField("championships", championships))
                # driver bio
                document.add(TextField("driver_bio", driver_bio, Field.Store.YES))
                # series description
                document.add(
                    TextField("series_description", series_description, Field.Store.YES)
                )
                # team description
                document.add(
                    TextField("team_description", team_description, Field.Store.YES)
                )

                logger.debug("Writting the new document.")
                writer.addDocument(document)
                writer.commit()

                logger.debug(f"Documents indexed: {writer.getDocStats().numDocs}")
    finally:
        writer.close()


def search_index(index_str_path: str, query: str):
    lucene.initVM()

    # Open index once
    index_path = File(index_str_path).toPath()
    index_store = FSDirectory.open(index_path)
    reader = DirectoryReader.open(index_store)
    searcher = IndexSearcher(reader)
    analyzer = StandardAnalyzer()

    text_search, arg_search, min_search, max_search = parse_query_args(query)
    logger.debug("Text search: %s", text_search)
    logger.debug("Arg search: %s", arg_search)
    logger.debug("Min search: %s", min_search)
    logger.debug("Max search: %s", max_search)

    fields_and_weights = {
        "content": 1.0,
        "driver_name": 3.0,
        "nationality": 3.0,
        "series": 3.0,
        "age": 3.0,
        "birthday": 3.0,
        "hometown": 3.0,
        "races_started": 3.0,
        "races_entered": 3.0,
        "wins": 3.0,
        "podiums": 3.0,
        "pole_positios": 3.0,
        "fastest_laps": 3.0,
        "race_win_percentage": 3.0,
        "podium_percentage": 3.0,
        "driverdb_score": 2.0,
        "current_team": 3.0,
        "all_teams": 2.0,
        "car_number": 1.0,
        "championships": 2.0,
        "driver_bio": 2.0,
        "team_description": 2.0,
        "series_description": 2.0,
    }

    builder = BooleanQuery.Builder()

    if text_search:
        text_search_builder = BooleanQuery.Builder()
        for field, weight in fields_and_weights.items():
            query_parser = QueryParser(field, analyzer)
            parsed_query = query_parser.parse(text_search)
            boosted_query = BoostQuery(parsed_query, float(weight))
            text_search_builder.add(boosted_query, BooleanClause.Occur.SHOULD)
        text_query = text_search_builder.build()
        builder.add(text_query, BooleanClause.Occur.MUST)

    for field, value in arg_search:
        arg_query_parser = QueryParser(field, analyzer)
        parsed_arg_query = arg_query_parser.parse(value)
        builder.add(parsed_arg_query, BooleanClause.Occur.MUST)

    for field, value in min_search:
        if "percentage" not in field:
            min_query = IntPoint.newRangeQuery(field, int(value), Integer.MAX_VALUE)
            builder.add(min_query, BooleanClause.Occur.MUST)
        else:
            min_query = DoublePoint.newRangeQuery(field, float(value), Double.MAX_VALUE)
            builder.add(min_query, BooleanClause.Occur.MUST)

    for field, value in max_search:
        if "percentage" not in field:
            min_query = IntPoint.newRangeQuery(field, Integer.MIN_VALUE, int(value))
            builder.add(min_query, BooleanClause.Occur.MUST)
        else:
            min_query = DoublePoint.newRangeQuery(field, Double.MIN_VALUE, float(value))
            builder.add(min_query, BooleanClause.Occur.MUST)

    combined_query = builder.build()
    hits = searcher.search(combined_query, 10)

    logger.info(f"Found {hits.totalHits} results.")

    for _, hit in enumerate(hits.scoreDocs, 1):
        doc = searcher.doc(hit.doc)
        print(f"Driver Name: {doc.get('driver_name')}")


if __name__ == "__main__":
    create_index(
        "/Users/martin/Development/school_projects/vinf/project-2/data/index",
        "/Users/martin/Development/school_projects/vinf/project-2/data/complete_data.tsv",
    )
    # print(parse_query_args("John Doe @current_team=red_bull ^age=20 $age=50"))
    # search_index("/Users/martin/Development/school_projects/vinf/project-2/data/index", "Verstappen")

