import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import operator
from decimal import *
from datetime import datetime
from datetime import timedelta
import os.path
import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler



getcontext().prec = 28
time_in_min = 30
number_of_coins_to_analyze = 300
number_of_results = 20

root = '/home/datalab/Desktop/CMC_Data/coin/'
beginning_time = '2018-01-16 19:52:25.195867'
origin_time = datetime.strptime('16/01/18 19:52', '%d/%m/%y %H:%M')

# root = '/home/datalab/Desktop/CMC_Data/coin2/'
# beginning_time = '2018-02-04 13:49:54.042765'
# origin_time = datetime.strptime('04/02/18 13:49', '%d/%m/%y %H:%M')

df_topCoins = pd.DataFrame()
df_bottomCoins = pd.DataFrame()



def analyze_coin(coin_files_1, coin_files_2):
    coins_ratio = {}

    previous = pd.read_json(coin_files_1)
    current = pd.read_json(coin_files_2)

    previous = previous[(previous['rank']) < number_of_coins_to_analyze]
    current = current[(current['rank']) < number_of_coins_to_analyze]

    count = 0
    for current_index, current_row in current.iterrows():
        for previous_index, previous_row in previous.iterrows():
            if current_row['name'] == previous_row['name']:
                ratio = float(current_row['market_cap_usd'] / previous_row['market_cap_usd'])
                coins_ratio[current_row['name']] = ratio
                count += 1

    sorted_list = sorted(coins_ratio.items(), key=operator.itemgetter(1))
    sorted_list.reverse()

    global origin_time, df_topCoins, df_bottomCoins
    column_label = origin_time

    coin_top_df = pd.DataFrame(sorted_list[:number_of_results])
    coin_top_df = coin_top_df.drop([1], axis=1)
    coin_top_df = coin_top_df.rename(columns={0: str(column_label)})
    df_topCoins = pd.concat([df_topCoins, coin_top_df], axis=1)

    coin_bottom_df = pd.DataFrame(sorted_list[-number_of_results:])
    coin_bottom_df = coin_bottom_df.drop([1], axis=1)
    coin_bottom_df = coin_bottom_df.rename(columns={0: str(column_label)})
    df_bottomCoins = pd.concat([df_bottomCoins, coin_bottom_df], axis=1)

    # Update time
    origin_time = origin_time + timedelta(minutes=time_in_min)

    # print df_topCoins, df_bottomCoins
    df_topCoins.to_html(open('./top_coins.html', 'w'))
    df_bottomCoins.to_html(open('./bottom_coins.html', 'w'))

    # df_topCoins.to_html(open('./top_coins2.html', 'w'))
    # df_bottomCoins.to_html(open('./bottom_coins2.html', 'w'))


def getFiles(time_in_min, directory):
    for root, dir, files in os.walk(directory):
        list.sort(files)
        i = 1

        while i < (len(files)+1):

            coin_files_1 = root+str(i)+'.json'
            i = i + time_in_min
            coin_files_2 = root+str(i)+'.json'

            print coin_files_1, coin_files_2
            if os.path.isfile(coin_files_1) and os.path.isfile(coin_files_2):
                analyze_coin(coin_files_1, coin_files_2)




getFiles(time_in_min, root)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    event_handler = LoggingEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()