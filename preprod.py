# @author ceren.yikilmaz
# this script finds databases whose message collection is not empty in mongo.
# then it gives the total device, open app count and active device count of the related apps.

# libraries

from os import sched_get_priority_max
from re import S
import pymongo
from pymongo.mongo_client import MongoClient
import os

# mongo connection string. 
# this is a variable, it gets the connection string from env.
# for local tests you can add a temporary enviroment variable like this;
# export python_env_mongo=mongodb://netmera-preprod:e82addefda53a123eb2e7e0e04cde41c@10.34.167.208:27017
# for permanent solution you can add this in your .bash_profile or .bashrc file.
# for production you must add this string in your .bashrc file

client = MongoClient(os.environ["python_env_mongo"])

dbs = client.database_names()

for db in dbs:

    mydb = client[db]
    mycol = mydb["message"]
    mycol2 = mydb["appStatsDaily"]
    count = mycol.count()

    query = mycol.find({"msgMethod" : "CAMPAIGN"},{"schedule.startDate":1,"startDate":1,"_id":0}).sort("startDate",-1).limit(1)

    if count > 0:

            print("App Key: " + db)

            for doc in query:   
                
                schedule = doc.get('schedule', None)

                schedule_start = None

                if schedule is not None:
                    schedule_start = schedule.get('startDate', None)

                start_date = doc.get('startDate', None)
                
                if schedule_start is not None:

                    print("Last Message: " + str(schedule_start))
                    
                    # you can write here different date, like {"year":2021,"month":11,"day":20}

                    for doc in mycol2.find({"year":2020,"month":12,"day":19},{"dailyActDev.TOTAL":1,"totalDev.TOTAL":1,"event.n:oa.TOTAL":1,"_id":0}):
                        
                        daily_active = doc.get('dailyActDev', None)
                        if daily_active is not None:
                            active_devices = daily_active.get('TOTAL', 0)
                            print("Daily Active: " + str(active_devices))

                        total_device = doc.get('totalDev', None)
                        if total_device is not None:
                            total = total_device.get('TOTAL', 0)
                            print("Total Device: " + str(total))

                        open_app = doc.get('event', None)
                        if open_app is not None:
                            oa = open_app.get('n:oa')
                            oa_total = oa.get('TOTAL')
                            print("Open App: " + str(oa_total))

                else:

                    print("Last Message: " + str(start_date))

                    for doc in mycol2.find({"year":2021,"month":12,"day":19},{"dailyActDev.TOTAL":1,"totalDev.TOTAL":1,"event.n:oa.TOTAL":1,"_id":0}):

                        daily_active = doc.get('dailyActDev', None)
                        if daily_active is not None:
                            active_devices = daily_active.get('TOTAL', 0)
                            print("Daily Active: " + str(active_devices))

                        total_device = doc.get('totalDev', None)
                        if total_device is not None:
                            total = total_device.get('TOTAL', 0)
                            print("Total Device: " + str(total))

                        open_app = doc.get('event', None)
                        if open_app is not None:
                            oa = open_app.get('n:oa')
                            oa_total = oa.get('TOTAL')
                            print("Open App: " + str(oa_total))

                

