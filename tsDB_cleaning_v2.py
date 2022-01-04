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

# mongo automationdan tsDB'si olan dbleri al.

for db in automationDbs:
    if db.startswith("tsDB"): # tsDB_fizy => ['tsDB', 'fizy']
        dbName = db.split("_")[1]
        automationDbNames.append(dbName)

#print(automationDbNames)

# mongo'da olan dblerini bul

for automationDbName in automationDbNames:
    for messageDbName in messageDbs:

        # iki yerde de varsa;

        if automationDbName == messageDbName:

            #print(automationDbName)
            #print(messageDbName)

            mydb = mongoMessageClient[messageDbName]
            #print(mydb)
            mycol = mydb["message"]
            #print(mycol)

            today = datetime.datetime.utcnow()
            nDayAgo = datetime.timedelta(days = 100)
            deltaDate = today - nDayAgo

            #message mongosuna bağlan

            for doc in mycol.find({"endDate" : {"$gt" : deltaDate}, "msgMethod" : "AUTOMATED", "sendStatus": "DEACTIVE"}, {"triggerId" : 1, "id" : 1}):
                #print("dbName: " + messageDbName + " Id: " + str(doc.get('_id')) + " TriggerId: " + str(doc.get('triggerId')))
                # automation mongoda count
                
                mydb2 = mongoAutomationClient["tsDB_" + messageDbName]
                #print(mydb2)
                mycol2 = mydb2["tsn_"+str(doc.get('triggerId'))]
                #print(mycol2)
                count = mycol2.count()
                #print(count)

                if count > 0:
                    print(mycol2)
                    print("dbName: " + messageDbName + " Id: " + str(doc.get('_id')) + " TriggerId: " + str(doc.get('triggerId')) + " Count: " + str(count) )
            


