import requests
import csv
import time, datetime
import multiprocessing
import math
import sqlite3
from geopy.distance import great_circle

stopsbyroute = {}

parameters = {"api_key": 'wX9NwuHnZU2ToO7GmGR9uw', #public api key
	      "format": 'json'}

#called once a day
def get_routes():
    all_routes = []
    
    response = requests.get("http://realtime.mbta.com/developer/api/v2/routes",
			    params=parameters)
    data = response.json()

    for route in data['mode']:
	if route['mode_name'] == 'Bus':
	    for r in route['route']:
		all_routes.append(str(r['route_id']))

    return all_routes

def get_predictions_info(stopids, route, c):
    print route
    parametersPred = {"route": route,
		      "max_time": 1440,
		      "max_trips": 100,
		      "api_key": 'wX9NwuHnZU2ToO7GmGR9uw',
		      "format": 'json'}

    for s in stopids:
	for d in [0,1]:
            parametersPred['stop'] = s
            parametersPred['direction'] = d
            response = requests.get("http://realtime.mbta.com/developer/api/v2/schedulebystop", params=parametersPred)
            data = response.json()
	    #print data
	    try:
		trips = data['mode'][0]['route'][0]['direction'][0]['trip']
	    except:
		continue
	    for t in trips:
		c.execute("INSERT INTO predictions VALUES (?, ?, ?, ?, ?, ?)",
			  (route, s, t['trip_id'], t['sch_arr_dt'],t['sch_dep_dt'],d))
    return

def set_predictions_table(all_routes, c, conn):
    for route in all_routes:
	parameters["route"] = route

	response = requests.get("http://realtime.mbta.com/developer/api/v2/stopsbyroute", params=parameters)
	data = response.json()
	    
	stopids = get_stops_info(data, route)
	get_predictions_info(stopids, route, c)
	conn.commit()
	time.sleep(30)
    return


def get_stops_info(data, route):
    stopids = []
    stopsbyroute[route] = [[],[]]
    for direct in data['direction']: #loop over direction info
	dirid = int(direct['direction_id'])
	for s in direct['stop']:
	    stopsbyroute[route][dirid].append([s['stop_lat'], s['stop_lon'], s['stop_order'], s['stop_id']])
	    stopids.append(s['stop_id'])
    return stopids

def stopped(current, c, conn):

    try:
	for routes in current['mode'][0]['route']:
	    route = str(routes['route_id'])
	    for d in routes['direction']:
		print "new d"
		print d

		pos1 = []
		pos2 = []
		tstamp = []
		dirid = int(d['direction_id'])
		for t in d['trip']:
		    print "new t"
		    print t

		    pos1 = t['vehicle']['vehicle_lat']
		    pos2 = t['vehicle']['vehicle_lon']
		    tstamp = t['vehicle']['vehicle_timestamp']
		    tripid = t['trip_id']

		    for i in range(len(stopsbyroute[route][dirid])):
		    #stopsbyroute[route][dirid][i][0] the lat of this stop&dir
			distance = great_circle((pos1, pos2), (stopsbyroute[route][dirid][i][0], stopsbyroute[route][dirid][i][1])).meters
			if (distance < 5):
			    c.execute("INSERT INTO data VALUES (?, ?, ?, ?, ?, ?, ?)", (route, dirid, stopsbyroute[route][dirid][i][0], stopsbyroute[route][dirid][i][1], tstamp, tripid, distance))
			    print "warning {} {} {} {}".format(pos1, pos2, stopsbyroute[route][dirid][i][0], stopsbyroute[route][dirid][i][1])
			    conn.commit()
			    #break
			    

    except:
	print current
    return 

def main():
    
    conn = sqlite3.connect('TEST.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE data           
                 (route text, direction real, stop real, stop_order real, time real, trip real,                 distance real)''')    


    #INSERT DAY INTO PREDICTIONS?
    c.execute('''CREATE TABLE predictions
                 (route text, stop real, trip real, arrival real, departure real, direction real)''')



    #run for a week                        
    for i in xrange(0,7):
        #sleep until 5 AM                                                       

	'''
        t = datetime.datetime.today()
        future = datetime.datetime(t.year, t.month, t.day, 7, 0)
        if t.hour >= 3:
            future += datetime.timedelta(days=1)
        time.sleep((future-t).total_seconds())
	'''


        #do 5AM stuff                                                           
	jobs = []

	queue = multiprocessing.Queue()

	all_routes = get_routes()
	all_routes = ['77']

	set_predictions_table(all_routes, c, conn)
	print stopsbyroute.keys()

	parameters["routes"] = '\'' + ','.join(all_routes) + '\''
	del parameters["route"]
	parameters["routes"] = '77'

	for t in range(2280):
	    try:
		response = requests.get("http://realtime.mbta.com/developer/api/v2/vehiclesbyroutes", params=parameters)
	    except requests.exceptions.ConnectionError:
		time.sleep(30)
		continue

	    #print(response.content)
	    #print response.status_code

	    if response.status_code == 200:
		data = response.json()
		#print data
		p = multiprocessing.Process(target=stopped, args=(data, c, conn, ))
		jobs.append(p)
		p.start()
		#p.join()
	    time.sleep(30)

    c.close()

if __name__ == '__main__':
    main()

