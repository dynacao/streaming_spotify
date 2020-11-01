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
        latest_date = pd.to_datetime('2020-10-30') # assign latest date here (daily data)
        earliest_date = pd.to_datetime('2017-01-01') # assign earliest date here (daily data)
        
        date_diff = int((latest_date - earliest_date) / np.timedelta64(1, 'D'))
        date_range = pd.date_range(latest_date, periods = date_diff+1, freq = '-1d') 
            # assign # of periods and the date interval
        date_range_str = [d.strftime('%Y-%m-%d') for d in date_range]
        
        url_list = [f'https://spotifycharts.com/regional/us/daily/{date_range_str[i]}' for i in list(range(date_diff+1))]
        
        # generatate archive urls
        for url in url_list:
            yield Request(url=url, callback=self.parse_archive_page)

    def parse_archive_page(self, response):
        date = [response.xpath('//div[@class="container"]/a/@href').extract()[0].split('/')[4]]*200

        song_title = response.xpath('//td[@class="chart-table-track"]//strong//text()')
        song_title = [t + '/sep/' if song_title.extract().index(t)!=199 else t for t in song_title.extract()]

        artist = response.xpath('//td[@class="chart-table-track"]//span//text()')
        artist = [a.replace('by ','') + '/sep/' if artist.extract().index(a)!=199 else a.replace('by ','') for a in artist.extract()]

        streams = response.xpath('//td[@class="chart-table-streams"]/text()')
        streams = [s.replace(',','') for s in streams.extract()]

        ranking = response.xpath('//td[@class="chart-table-position"]//text()')
        ranking = [r for r in ranking.extract()]

        movement = response.xpath('//rect|//circle|//polygon').xpath('name()')
            # 'rect' = rank unchanged, 'circle' = new entry, 'polygon' = ranked up/down
        movement = [m for m in movement.extract()]

        item = SpotifyItem()
        item['song_title']  = song_title
        item['artist']      = artist
        item['date']        = date
        item['streams']     = streams
        item['ranking']     = ranking
        item['movement']    = movement
        yield item