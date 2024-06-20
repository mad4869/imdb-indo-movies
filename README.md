# IMDB Indonesian Movies Scraper

A web scraping project using Python and Scrapy.

## Objectives

Extracting data regarding Indonesian movies from IMDb and storing it into:

1. a CSV file
2. a JSON file
3. a database file

## Preparation

### Define the Items

The items required for this project are defined within the `items.py` file.

```py
class IndomoviescraperItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    year = scrapy.Field()
    runtime = scrapy.Field()
    genre = scrapy.Field()
    director = scrapy.Field()
    stars = scrapy.Field()
    rating = scrapy.Field()
    imdb_score = scrapy.Field()
    imdb_votes = scrapy.Field()
    metascore = scrapy.Field()
    gross = scrapy.Field()
```

### Set Up the Spider

The spider is set up to crawl data from the domain `https://www.imdb.com/search/title/?country_of_origin=ID` and all subsequent pages.

### Build the Pipelines

The pipelines are required to:

1. Transform the data acquired by the spider
2. Store the data in a database

#### Transformation Pipeline

The transformation pipeline ensures that the data is shaped according to the project's requirements before being stored.

#### DB Pipeline

The DB pipeline connects the spider to a SQLite database and stores the data there.

## Execution

The spider is run with the command:

```bash
scrapy crawl indomoviespider -O /data/indomovie-data.csv
```

or

```bash
scrapy crawl indomoviespider -O /data/indomovie-data.json
```
