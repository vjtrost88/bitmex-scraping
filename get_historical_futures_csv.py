import json
import requests
import datatable as dt
import pandas as pd  # literally just for selecting the most recent start time if things fail
import time    # need this for sleeping and comparing timestamps
import os      # need this for checking if a file exists
from pathlib import Path  # same as above
import sys
import datetime

### DEFINE GLOBALS ###
fname = 'XBT_2020-01-01_2020-01-31.csv'
final_time = '2020-01-31 23:59:00.000000'
trade_url = 'https://www.bitmex.com/api/v1/trade'
bucket_url = 'https://www.bitmex.com/api/v1/trade/bucketed'
symbol = 'XBT:perpetual'

### DEFINE FUNCTIONS ###
def processRequest(r):
    '''
        Take in a request object and parse it to return the following:
            temp: a data table for appending
            remaining: number of requests remaining for us
    '''
    try:
        print(r.url)
        # check the headers to find out where we're at in our asking
        print(f"Requests remaining: {r.headers['x-ratelimit-remaining']}")
        if r.headers['x-ratelimit-remaining'] == '0':
            chillOut(r)
        temp = dt.Frame(r.json())
        return temp
    except:
        print(r.status_code)
        print(r.reason)

def writeFile(temp, fname):
    # if the file exists, append it. If not, write outright
    if Path(fname).is_file():
        temp.to_csv(fname, append=True)    # if append=True, headers are not included (nice)
    else:
        temp.to_csv(fname)
        
def chillOut(r):
    # sleep till rate limit resets
    print("Exceeded rate limit. Going to wait NOW.")
    while time.time() < float(r.headers['x-ratelimit-reset']):
        print("Sleeping for " + str(float(r.headers['x-ratelimit-reset']) - time.time()))
        time.sleep(float(r.headers['x-ratelimit-reset']) - time.time())


# check if we've made progress so we don't re-query data we already have
if Path(fname).is_file():
    dat = pd.read_csv(fname)
    dat['timestamp'] = pd.to_datetime(dat['timestamp'])
    start_time = str(dat['timestamp'].max())
    # make end_time the same thing because we're gonna add a min to it in the loop
    end_time = start_time.split("+")[0]
    # for whatever reason, there's this +00:00 bit at the end that causes an invalid format error. strip it
    start_time = start_time.split("+")[0]
    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")
    # add a millisecond because if it's even on the 0, the bucket endpoint will use that as the end of one bucket
    start_time = start_time + datetime.timedelta(milliseconds=1)
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    print("Starting here: " + start_time)
else:
    start_time = '2020-01-01 00:00:00.001'
    end_time = '2020-01-01 00:00:00.000'


# PSEUDO CODE
# make a request to the trade endpoint
    # request still has status_code and reason attrs
    
    
# get data until we hit the final_time
while end_time != final_time:
    
    # add a minute to end time to get information about 1m bucket of trades
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f")
    end_time = end_time + datetime.timedelta(minutes=1)
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    # start time already has a millisecond added to it to avoid getting the 1m bucket before that
    
    print("Start time: " + str(start_time))
    print("End time: " + str(end_time))

    # get information about what we should get
    print("Making trade info request...")
    trade_info = requests.get(f'{bucket_url}?binSize=1m&symbol={symbol}&count=1&startTime={start_time}&endTime={end_time}')
    # don't exceed rate limit
    # TODO issue here where this is no header returned, idk if that's a me-thing or them-thing
    if trade_info.headers['x-ratelimit-remaining'] == '0':
        chillOut(trade_info)
    # error handling in case this call doesn't work
    if trade_info.status_code != 200:
        print(trade_info.reason)
        print(trade_info.json()['error']['message'])
        sys.exit()
        
    # get the first record from this trade_info response and get the number of trades that happened in that minute
    num_trades = trade_info.json()[0]['trades']
    print(trade_info.json()[0]['timestamp'] + ": " + str(num_trades))
    
    # this will let us know how many requests of size 1000 need to happen
    pages = (num_trades // 1000) + 1
    print(str(pages) + " pages.")
    
    # if there's less than 1000, use the raw number of trades as the count
    if pages == 1:
        count = num_trades
        print("Making trade request...")
        r = requests.get(f'{trade_url}?symbol={symbol}&count={count}&startTime={start_time}&endTime={end_time}')
        # build the temporary dt, return number of requests remaining
        temp = processRequest(r)

        # then write the new data to file
        writeFile(temp, fname)
    
    # otherwise, loop through the number of pages and get the data
    else:
        for i in range(pages):
            print("Making trade request " + str(i+1))
            count = 1000
            r = requests.get(f'{trade_url}?symbol={symbol}&count={count}&start={count*(i)}&startTime={start_time}&endTime={end_time}')
            # build the temporary dt, return number of requests remaining
            temp = processRequest(r)

            # then write the new data to file
            writeFile(temp, fname)

            
    # increment starting time to catch up with end time
    start_time = end_time
    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")   # back to datetime obj
    # add a millisecond because if it's even on the 0, the bucket endpoint will use that as the end of one bucket
    start_time = start_time + datetime.timedelta(milliseconds=1)
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    
print("Finished.")

    
    
