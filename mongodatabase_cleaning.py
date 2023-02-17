import collections
from os import lseek, sched_get_priority_max
from re import M, S
from pymongo.mongo_client import MongoClient
import os
import datetime
import logging

# ********* logging *********

logging.basicConfig(filename="tsDB_cleaning.log",
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    filemode='w')

log = logging.getLogger()

log.setLevel(logging.DEBUG)

# ********* mongo clients *********

mongoAutomationClient = MongoClient(os.environ["python_env_mongo"])
automationDbs = mongoAutomationClient.list_database_names()
automationDbNames = []

mongoMessageClient = MongoClient(os.environ["python_env_mongo2"])
messageDbs = mongoMessageClient.list_database_names()

# Get databases in MongoAutomation, which have tsDB collection, split and get appkey

for db in automationDbs:
    if db.startswith("tsDB"): 
        dbName = db.split("_")[1]
        automationDbNames.append(dbName)

# Compare clients, which have tsDB database in MongoAutomation and also have databse in Mongo

for automationDbName in automationDbNames:
    for messageDbName in messageDbs:
        if automationDbName == messageDbName:

            mydb = mongoMessageClient[messageDbName]
            mycol = mydb["message"]

            today = datetime.datetime.utcnow()
            nDayAgo = datetime.timedelta(days = 150)
            deltaDate = today - nDayAgo

            for doc in mycol.find({"endDate" : {"$lt" : deltaDate}, "msgMethod" : "AUTOMATED", "sendStatus": "DEACTIVE"}, {"triggerId" : 1, "id" : 1}):
                                
                mydb2 = mongoAutomationClient["tsDB_" + messageDbName]
                mycol2 = mydb2["tsn_"+str(doc.get('triggerId'))]
                count = mycol2.count()

                # Split tsn_name for printing.

                tsn_name = str(mycol2).split(" ")[5]
                tsn_name = tsn_name.split("'")[1]

                if count > 0:

                    log.info("dbName: " + messageDbName + " Id: " + str(doc.get('_id')) + " TriggerId: " + str(doc.get('triggerId')) + " Count: " + str(count) + " tsnName: " + tsn_name)
                    log.info(tsn_name + " collection in tsDB_" + messageDbName + " is dropping..")

                    mycol2.drop()

                    log.info(tsn_name + " collection in tsDB_" + messageDbName + " is dropped!")

                else:

                    log.info("dbName: " + messageDbName + " Id: " + str(doc.get('_id')) + " TriggerId: " + str(doc.get('triggerId')) + " Count: " + str(count) + " tsnName: " + tsn_name)
                    log.info("Count equal zero, skipping.." + " tsnName: " + tsn_name)
