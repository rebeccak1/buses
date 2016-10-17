import requests
import plotly.plotly as py
from plotly.graph_objs import *
import plotly.tools as tls
import plotly.graph_objs as go

import numpy as np
import time

from scipy.special import gamma as Gamma
import datetime

import sqlite3

import itertools

def unique(seq):
    myset = set(seq)
    return list(myset)

def findTimeDiff(stoptimes, predtimes):

    t_diff = []
    p_times = {trip: arr for trip, arr in predtimes}

    #find last time at stop
    filteredstops = map(lambda g: max(g, key=lambda x: x[1]), [list(j) for i, j in itertools.groupby(stoptimes, lambda y: y[0])])
    print filteredstops

    
    for trip, time, distance in filteredstops:
	try:
	    t_diff.append((datetime.datetime.utcfromtimestamp(p_times[trip])-datetime.datetime.utcfromtimestamp(time)).total_seconds()/60)
	    print (datetime.datetime.utcfromtimestamp(p_times[trip])-datetime.\
datetime.utcfromtimestamp(time)).total_seconds()/60
	except:
	    continue

    return t_diff
    #return map(lambda t1,t2: (datetime.datetime.utcfromtimestamp(t2[0])-datetime.datetime.utcfromtimestamp(t1[0])).total_seconds(), predtimes, filteredstops)
	
if __name__ == '__main__':

    conn = sqlite3.connect('test.db')

    c = conn.cursor()

    #are stops different for each direction?????
    c.execute('SELECT {col} FROM {tn} WHERE {cn}=1'.
	      format(tn='data', col='stop', cn='direction'))
    allstops = unique(c.fetchall())
    print allstops

    stoptimes = {}

    for s in allstops:
	for d in [1]:#for d in [0,1]:
	    c.execute('SELECT trip, time, distance FROM data WHERE stop=? AND direction=?', (str(s[0]), str(d))) 
	    s_times = c.fetchall()

	    #c.execute('SELECT trip, arrival FROM predictions WHERE stop=? AND direction=?', (str(s[0]), str(d), ))
	    c.execute('SELECT trip, arrival FROM predictions WHERE stop=?', (str(s[0]), ))
	    p_times = c.fetchall()

	    stoptimes[s[0]] = findTimeDiff(s_times, p_times)


    stoptimes = list(itertools.chain(*stoptimes.values()))
    
    conn.close()

    stream_ids = tls.get_credentials_file()['stream_ids']

    # (!) Get stream id from stream id list, 
    #     only one needed for this plot
    stream_id = stream_ids[0]

# Make a stream id object linking stream id to 'token' key
    stream = Stream(token=stream_id)

# no need to set `'maxpoints'` for this plot


    trace = go.Histogram(
	x = stoptimes,
	stream=stream
    )

    data = go.Data([trace])

    fig = Figure(data=data)

    unique_url = py.plot(fig, filename='71-timediff')

    s = py.Stream(stream_id)
    s.open()

    time.sleep(5)
    '''
    the prior values. If it comes every 20 min, overthe course of an hour, 
    we will see 3 buses.
    '''
    shape = 3 #number of observations 
    rate = 60 #sum of observations

    for stimes in stoptimes:

	shape += 1
	rate += stimes

	s_data['y'] = Gamma(shape, scale = 1.0/rate)
	s.write(s_data)
	time.sleep(5)

    s.close()
