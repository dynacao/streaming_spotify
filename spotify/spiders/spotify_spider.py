from scrapy import Spider, Request
from spotify.items import SpotifyItem
from datetime import datetime
import pandas as pd
import numpy as np

class SpotifySpider(Spider):
    name = 'spotify_spider'
    start_urls = ['https://spotifycharts.com/regional/us/weekly/latest']
    allowed_urls = ['https://spotifycharts.com']

    def parse(self, response):
        latest_date = pd.to_datetime('2020-10-29') # assign latest date here (daily data)
        #latest_date = pd.to_datetime('2020-10-22') # assign latest date here (weekly data)
        earliest_date = pd.to_datetime('2017-01-01') # assign earliest date here (daily data)
        date_diff = int((latest_date - earliest_date) / np.timedelta64(1, 'D'))
        date_range = pd.date_range(latest_date, periods = date_diff+1, freq = '-1d')
        #date_range = pd.date_range(latest_date, periods = date_diff, freq = '-7d')
        date_range_str = [d.strftime('%Y-%m-%d') for d in date_range]
        url_list = [f'https://spotifycharts.com/regional/us/daily/{date_range_str[i]}' for i in list(range(date_diff))]
        url_list = [f'https://spotifycharts.com/regional/us/daily/{date_range_str[i]}--{date_range_str[i]}' for i in list(range(date_diff))]
        
        # generatate archive urls
        for url in url_list[0:5]:
            yield Request(url=url, callback=self.parse_archive_page)

    def parse_archive_page(self, response):
        week_of = response.xpath('//div[@class="container"]/a/@href').extract()[0].split('/')[4]

        song_title = response.xpath('//td[@class="chart-table-track"]//strong//text()')
        song_title = [s + '/sep/' for s in song_title.extract()]

        artist = response.xpath('//td[@class="chart-table-track"]//span//text()')
        artist = [a.replace('by ','') + '/sep/' for a in artist.extract()]

        streams = response.xpath('//td[@class="chart-table-streams"]/text()')
        streams = [s.replace(',','') for s in streams.extract()]

        ranking = response.xpath('//td[@class="chart-table-position"]//text()')
        ranking = [r for r in ranking.extract()]

        item = SpotifyItem()
        item['song_title'] = song_title
        item['artist'] = artist
        item['week_of'] = week_of
        item['streams'] = streams
        item['ranking'] = ranking
        yield item