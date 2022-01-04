import collections
from os import sched_get_priority_max
from re import M, S
from pymongo.mongo_client import MongoClient
import os
import datetime

mongoAutomationClient = MongoClient(os.environ["python_env_mongo"])
automationDbs = mongoAutomationClient.list_database_names()
automationDbNames = []

mongoMessageClient = MongoClient(os.environ["python_env_mongo2"])
messageDbs = mongoMessageClient.list_database_names()

# Get dbs which have tsDB collection

for db in automationDbs:
    if db.startswith("tsDB"): 
        dbName = db.split("_")[1]
        automationDbNames.append(dbName)

# Find dbs which exists in Mongo

for automationDbName in automationDbNames:
    for messageDbName in messageDbs:

        # If it's exists both

        if automationDbName == messageDbName:

            mydb = mongoMessageClient[messageDbName]
            mycol = mydb["message"]

            today = datetime.datetime.utcnow()
            nDayAgo = datetime.timedelta(days = 100)
            deltaDate = today - nDayAgo

            # Connect to message mongo 

            for doc in mycol.find({"endDate" : {"$gt" : deltaDate}, "msgMethod" : "AUTOMATED", "sendStatus": "DEACTIVE"}, {"triggerId" : 1, "id" : 1}):
                                
                mydb2 = mongoAutomationClient["tsDB_" + messageDbName]
                mycol2 = mydb2["tsn_"+str(doc.get('triggerId'))]
                count = mycol2.count()

                if count > 0:
                    print(mycol2)
                    print("dbName: " + messageDbName + " Id: " + str(doc.get('_id')) + " TriggerId: " + str(doc.get('triggerId')) + " Count: " + str(count) )
            


