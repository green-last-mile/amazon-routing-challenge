import os, json, sys, math
from collections import OrderedDict

def markFailedRoutes(routes, packages):
# Return array marking routes with failed deliveries.
    rfail = []
    for id in routes:
        attempt = 0
        pack = packages[id]
        for s in pack:
            stop = pack[s]
            for package in stop:
                if "scan_status" in stop[package] and stop[package]['scan_status'] == 'DELIVERY_ATTEMPTED':
                    attempt = 1
                    break
            if attempt == 1: break
        rfail.append(attempt)
    return rfail

def getRouteList(routes, noPrune, failedList):
    rList = []
    r = 0
    for id in routes:
        route = routes[id]
        if ( (len(failedList) == 0) or (failedList[r]==0)) and (noPrune or (route['route_score']=='High')):
            rList.append(id)
        r = r+1
    return rList
