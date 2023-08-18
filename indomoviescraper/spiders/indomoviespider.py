import scrapy

from indomoviescraper.items import IndomoviescraperItem


class IndomovieSpider(scrapy.Spider):
    name = "indomoviespider"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/search/title/?country_of_origin=ID"]

    def parse(self, response):
        movies = response.css(".lister-item")

        for movie in movies:
            item = IndomoviescraperItem()

            item["title"] = movie.css(".lister-item-header a::text").get()

            yield item
