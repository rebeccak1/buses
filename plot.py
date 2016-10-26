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
    #print filteredstops

    
    for trip, time, distance in filteredstops:
	try:
	    now = datetime.datetime.now()
	    predict = datetime.datetime.utcfromtimestamp(p_times[trip])
	    actual = datetime.datetime.utcfromtimestamp(time)
	    diff = datetime.datetime(now.year, now.month, now.day, actual.hour, actual.minute, actual.second) - datetime.datetime(now.year, now.month, now.day, predict.hour, predict.minute, predict.second) 
	    diff = diff.total_seconds()/60
	    if(diff < -1400):
		continue
	    t_diff.append(diff)
	    print diff
	
	    #t_diff.append(((datetime.datetime.utcfromtimestamp(p_times[trip])-datetime.datetime.utcfromtimestamp(time)).total_seconds() % 86400)/60)
	except:
	    continue

    return t_diff
    #return map(lambda t1,t2: (datetime.datetime.utcfromtimestamp(t2[0])-datetime.datetime.utcfromtimestamp(t1[0])).total_seconds(), predtimes, filteredstops)
	
if __name__ == '__main__':

    conn = sqlite3.connect('test3.db')

    c = conn.cursor()

    #get all routes
    c.execute('SELECT {col} FROM {tn}'.
	      format(tn='data', col='route'))
    all_routes = unique(c.fetchall())


    all_times = {}
    for route in all_routes:
	print str(route[0])
    
    #are stops different for each direction?????
	c.execute('SELECT {col} FROM {tn} WHERE {cn}=1 AND route=?'.
		  format(tn='data', col='stop', cn='direction'),(str(route[0]),))
	allstops = unique(c.fetchall())
	print allstops

	stoptimes = {}

	for s in allstops:
	    for d in [1]:#for d in [0,1]:
		c.execute('SELECT trip, time, distance FROM data WHERE stop=? AND direction=? AND route=?', (str(s[0]), str(d), str(route[0]), )) 
		s_times = c.fetchall()

		c.execute('SELECT trip, arrival FROM predictions WHERE stop=? AND direction=? AND route=?', (str(s[0]), str(d), str(route[0]), ))
		p_times = c.fetchall()

		stoptimes[s[0]] = findTimeDiff(s_times, p_times)


	stoptimes = list(itertools.chain(*stoptimes.values()))
	all_times[route] = stoptimes
    
    conn.close()

    traces = []
    print len(all_times)
    for key, value in all_times.items():
	traces.append(go.Histogram(
	             x = value,
		     name = str(key[0])
		     ))

    data = traces

    layout = go.Layout(
	legend=dict(orientation="v")
	)
    fig = go.Figure(data=data, layout=layout)

    unique_url = py.plot(fig, filename='all-routes-timediff')

