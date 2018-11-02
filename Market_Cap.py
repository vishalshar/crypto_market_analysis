import requests
import json
import time
from datetime import datetime


# API Start and limit
start = 0
limit = 1200

count = 1


print str(datetime.now())

while True:
    coins_apiLink = 'https://api.coinmarketcap.com/v1/ticker/?start=%s&limit=%s'%(start,limit)
    coins_request = requests.get(coins_apiLink)
    coins = coins_request.json()

    global_apiLink = 'https://api.coinmarketcap.com/v1/global/'
    global_request = requests.get(global_apiLink)

    print "status :", global_request.status_code, coins_request.status_code
    global_market = global_request.json()

    coin_outputFile = '/home/datalab/Desktop/CMC_Data/coin2/%s.json'%(count)
    market_outputFile = '/home/datalab/Desktop/CMC_Data/global2/%s.json'%(count)

    with open(coin_outputFile, 'w') as fp:
        json.dump(coins, fp)

    with open(market_outputFile, 'w') as fp:
        json.dump(global_market, fp)

    time.sleep(60)
    count += 1