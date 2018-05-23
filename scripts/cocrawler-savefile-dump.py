import sys

import cocrawler
import cocrawler.config as config

f = sys.argv[1]
config.config(None, None)
crawler = cocrawler.Crawler(load=f)

# at this point the crawler won't start until we call loop.run_until_complete ...

if sys.argv[2] == 'frontier':
    crawler.scheduler.dump_frontier()
