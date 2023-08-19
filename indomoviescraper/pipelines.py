# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3
import json
import logging


class IndomoviescraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Complete the url
        MAIN_URL = "https://www.imdb.com"
        movie_url = adapter.get("url")
        adapter["url"] = MAIN_URL + movie_url

        # Remove title, description whitespace
        title = adapter.get("title")
        if title is not None:
            new_title = title.strip()
            adapter["title"] = new_title
        desc = adapter.get("description")
        if desc is not None:
            new_desc = desc.strip()
            adapter["description"] = new_desc

        # Remove year parenthesis
        year = adapter.get("year")
        if year is not None:
            new_year = year.strip("()")
            adapter["year"] = new_year

        # Convert runtime to integer
        runtime = adapter.get("runtime")
        if runtime is not None:
            new_runtime = runtime.split(" ")[0]
            runtime_int = int(new_runtime)
            adapter["runtime"] = runtime_int

        # Remove genre whitespace
        # Turn text into list
        genre = adapter.get("genre")
        if genre is not None:
            new_genre = genre.strip()
            genre_list = new_genre.split(", ")
            adapter["genre"] = genre_list

        # Turn IMDB score into float
        imdb_score = adapter.get("imdb_score")
        if imdb_score is not None:
            imdb_score_float = float(imdb_score)
            adapter["imdb_score"] = imdb_score_float

        # Turn Metascore into integer
        metascore = adapter.get("metascore")
        if metascore is not None:
            metascore_int = int(metascore)
            adapter["metascore"] = metascore_int

        # Turn gross into real number
        gross = adapter.get("gross")
        if gross is not None:
            new_gross = gross.strip("$M")
            gross_num = int(float(new_gross) * 1_000_000)
            adapter["gross"] = gross_num

        return item


class SaveToDBPipeline:
    def __init__(self):
        self.con = sqlite3.connect("db/indomovies.db")
        self.cur = self.con.cursor()

        try:
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS movies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    description TEXT,
                    year VARCHAR(50),
                    url VARCHAR(255),
                    runtime INTEGER,
                    genre TEXT,
                    director VARCHAR(255),
                    stars TEXT,
                    rating VARCHAR(50),
                    imdb_score REAL,
                    metascore INTEGER,
                    gross INTEGER
                )
                """
            )
        except Exception as error:
            logging.error(f"Database initialization error: {error}")

            self.con.rollback()

    def process_item(self, item, spider):
        try:
            self.cur.execute("SELECT * FROM movies WHERE title = ?", (item["title"],))

            result = self.cur.fetchone()
            if result:
                spider.logger.warn(f'This title "{item["title"]}" already exists')
            else:
                genre = json.dumps(item["genre"])
                stars = json.dumps(item["stars"])

                self.cur.execute(
                    """
                    INSERT INTO movies(
                        title,
                        description,
                        year,
                        url,
                        runtime,
                        genre,
                        director,
                        stars,
                        rating,
                        imdb_score,
                        metascore,
                        gross
                    ) VALUES(
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?
                    )
                    """,
                    (
                        item["title"],
                        item["description"],
                        item["year"],
                        item["url"],
                        item["runtime"],
                        genre,
                        item["director"],
                        stars,
                        item["rating"],
                        item["imdb_score"],
                        item["metascore"],
                        item["gross"],
                    ),
                )
                self.con.commit()
        except Exception as error:
            logging.error(f"Data insertion error: {error}")

            self.con.rollback()
        finally:
            return item

    def close_spider(self, spider):
        self.cur.close()
        self.con.close()
