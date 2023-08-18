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

            item["url"] = movie.css(".lister-item-header a::attr(href)").get()
            item["title"] = movie.css(".lister-item-header a::text").get()

            item["imdb_score"] = movie.css(".ratings-imdb-rating strong::text").get()
            if item["imdb_score"] is not None:
                item["description"] = movie.css(
                    ".lister-item-content > p:nth-child(4)::text"
                ).get()
                item["director"] = movie.css(
                    ".lister-item-content p:nth-child(5) a:first-child::text"
                ).get()
                item["stars"] = movie.css(
                    ".lister-item-content p:nth-child(5) a:nth-child(n+2)::text"
                ).getall()
            else:
                item["description"] = movie.css(
                    ".lister-item-content > p:nth-child(3)::text"
                ).get()
                item["director"] = movie.css(
                    ".lister-item-content p:nth-child(4) a:first-child::text"
                ).get()
                item["stars"] = movie.css(
                    ".lister-item-content p:nth-child(4) a:nth-child(n+2)::text"
                ).getall()

            item["year"] = movie.css(".lister-item-year::text").get()
            item["runtime"] = movie.css(".runtime::text").get()
            item["genre"] = movie.css(".genre::text").get()
            item["rating"] = movie.css(".certificate::text").get()
            item["metascore"] = movie.css(
                ".ratings-metascore span.metascore::text"
            ).get()
            item["gross"] = movie.css(
                ".sort-num_votes-visible span:nth-child(5)::text"
            ).get()

            yield item

        next_page = response.css("a.lister-page-next.next-page::attr(href)").get()
        if next_page is not None:
            MAIN_URL = "https://www.imdb.com"
            next_page_url = MAIN_URL + next_page

            yield response.follow(next_page_url, callback=self.parse)
