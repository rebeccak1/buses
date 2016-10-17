import requests
import csv
import time, datetime
import multiprocessing
import math
import sqlite3

stops = [{},{}]

def getPredictionsInfo(stopids, c):
    parametersPred = {"route": 72,
		      "max_time": 1440,
		      "max_trips": 100,
		      "datetime": 1476608400,
		      "api_key": 'wX9NwuHnZU2ToO7GmGR9uw',
		      "format": 'json'}

    for s in stopids:
	for d in [0,1]:
            parametersPred['stop'] = s
            parametersPred['direction'] = d
            response = requests.get("http://realtime.mbta.com/developer/api/v2/schedulebystop", params=parametersPred)
            data = response.json()

	    try:
		trips = data['mode'][0]['route'][0]['direction'][0]['trip']
	    except IndexError:
		continue
	    for t in trips:
		c.execute("INSERT INTO predictions VALUES (?, ?, ?, ?, ?)",
			  (s, t['trip_id'], t['sch_arr_dt'],t['sch_dep_dt'],d))
    return

def getStopsInfo(data):
    stopids = []
    for direct in data['direction']: #loop over direction info
	i = 0

	dirid = int(direct['direction_id'])
	#stops[dirid] = {}
	for s in direct['stop']:
	    #stops[s['stop_order']] = [s['stop_lat'], s['stop_lon']]
	    stops[dirid][i] = [s['stop_lat'], s['stop_lon'], s['stop_order'], s['stop_id']]
	    stopids.append(s['stop_id'])
	    i = i + 1
    return stopids

def getDistance(lat1, lon1, lat2, lon2):
    lat1 = math.radians(float(lat1))
    lon1 = math.radians(float(lon1))
    lat2 = math.radians(float(lat2))
    lon2 = math.radians(float(lon2))
    R = 6371*10*3 #radius in m
    x = (lon2 - lon1) * math.cos(0.5*(lat2+lat1))
    y = lat2 - lat1
    d = R*math.sqrt(x*x + y*y)

    return d
    
def stopped(current, c, conn):

    pos1 = []
    pos2 = []
    tstamp = []
    
    for d in current['direction']:

	dirid = int(d['direction_id'])
	for t in d['trip']:
	    pos1.append(t['vehicle']['vehicle_lat'])
	    pos2.append(t['vehicle']['vehicle_lon'])
	    tstamp.append(t['vehicle']['vehicle_timestamp'])
	    tripid = t['trip_id']

	for i in range(len(stops[dirid])):
	    distance = getDistance(pos1[0], pos2[0], stops[dirid][i][0], stops[dirid][i][1])
	    if (distance < 5):

		c.execute("INSERT INTO data VALUES (?, ?, ?, ?, ?, ?)",
                          (dirid, stops[dirid][i][3], stops[dirid][i][2], tstamp[0], tripid, distance))
		conn.commit()
		break

    return 

def main():
    conn = sqlite3.connect('stoptimes.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE data                                                
                 (direction real, stop real, stop_order real, time real, trip real, 
                 distance real)''')    

    c.execute('''CREATE TABLE predictions
                 (stop real, trip real, arrival real, departure real, direction real)''')


    #run for a week                        
    for i in xrange(0,7):
        #sleep until 5 AM                                                       


        t = datetime.datetime.today()
        future = datetime.datetime(t.year, t.month, t.day, 7, 0)
        if t.hour >= 5:
            future += datetime.timedelta(days=1)
        time.sleep((future-t).total_seconds())



        #do 5AM stuff                                                           
	jobs = []

	queue = multiprocessing.Queue()

	parameters = {"route": 72,
		      "api_key": 'wX9NwuHnZU2ToO7GmGR9uw',
		      "format": 'json'}	      

	response = requests.get("http://realtime.mbta.com/developer/api/v2/stopsbyroute", params=parameters)
	data = response.json()

	stopids = getStopsInfo(data)

	getPredictionsInfo(stopids, c)
	conn.commit()



    #i = 0
	for t in range(2280):
	    try:
		response = requests.get("http://realtime.mbta.com/developer/api/v2/vehiclesbyroute", params=parameters)
	    except requests.exceptions.ConnectionError:
		time.sleep(30)
		continue



	    print(response.content)
	    print response.status_code

	    if response.status_code == 200:
		data = response.json()
		print data
		p = multiprocessing.Process(target=stopped, args=(data, c, conn, ))
		jobs.append(p)
		p.start()
		p.join()
	    time.sleep(30)

    c.close()

if __name__ == '__main__':
    main()

