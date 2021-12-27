import collections
from os import sched_get_priority_max
from re import M, S
from pymongo.mongo_client import MongoClient
import os
import datetime

mongoAutomationClient = MongoClient(os.environ["python_env_mongo"])
automationDbs = mongoAutomationClient.database_names()
automationDbs_2 = " "

mongoMessageClient = MongoClient(os.environ["python_env_mongo2"])
messageDbs = mongoMessageClient.database_names()

# mongo automationdan tsDB'si olan dbleri al.

f= open('AutomationDbs.txt', 'w')
for db in automationDbs:
    if db.startswith("tsDB"):
        automationDbs_2 = db.split("_")
        f.write(automationDbs_2[1] + "\n") 
f.close()


f= open('AutomationDbs.txt', 'r')
file_lines = f.read()
list_of_lines = file_lines.split("\n")

# mongo'da olan dblerini bul

f= open('MessageDbs.txt', 'w')
for automationDbName in list_of_lines:
    for messageDbName in messageDbs:

        # iki yerde de varsa;

        if automationDbName == messageDbName:
            f.write(messageDbName + "\n")

            mydb = mongoMessageClient[messageDbName]
            mycol = mydb["message"]

            today = datetime.datetime.utcnow()
            nDayAgo = datetime.timedelta(days = 30)
            deltaDate = today - nDayAgo

            #message mongosuna bağlan

            for doc in mycol.find({"endDate" : {"$lt" : deltaDate}, "msgMethod" : "AUTOMATED", "sendStatus": "DEACTIVE"}, {"triggerId" : 1, "id" : 1}):
                #print("dbName: " + messageDbName + " Id: " + str(doc.get('_id')) + " TriggerId: " + str(doc.get('triggerId')))
                #print(mycol)

                # automation mongoda count
                
                mydb2 = mongoAutomationClient["tsDB_" + messageDbName]
                #print(mydb2)
                mycol2 = mydb2["tsn_"+str(doc.get('triggerId'))]
                #print(mycol2)
                count = mycol2.count()
                #print(count)

                if count > 0:
                    print(mycol2)

