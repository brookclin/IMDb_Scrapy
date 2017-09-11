import scrapy
import time


class QuotesSpider(scrapy.Spider):
    name = "imdb"
    docid = 0

    def start_requests(self):
        urls = [
            'http://www.imdb.com/list/ls057823854/'  # 9954
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def title_parse(self, response):
        if self.docid > 2500:
            raise scrapy.exceptions.CloseSpider('2500 pages crawled.')
        output_dict = dict()
        output_dict['doc_id'] = self.docid
        self.docid += 1
        output_dict['url'] = response.url
        output_dict['timestamp_crawl'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        output_dict['raw_content'] = response.body
        yield output_dict

    def parse(self, response):
        titles = response.css('div.list.detail div.list_item div.info a::attr(href)').re(r'/title/(.*)/')
        titles = set(titles)
        for title in titles:
            url = 'http://www.imdb.com/title/' + title
            yield scrapy.Request(url, callback=self.title_parse)

        next_page = response.css('div.pagination a:contains("Next")::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
