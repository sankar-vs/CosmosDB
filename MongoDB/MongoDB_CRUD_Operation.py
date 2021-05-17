'''
@Author: Sankar
@Date: 2021-05-17 07:52:25
@Last Modified by: Sankar
@Last Modified time: 2021-05-17 13:58:09
@Title : Besic CRUD Operation in MongoDB
'''
import os
import json
import pymongo
from decouple import config
from log import logger

class mongoDB:

    def __init__(self):
        self.conString = config('conString')
        self.dbConnection()

    def dbConnection(self):
        try:
            self.client = pymongo.MongoClient(self.conString)
            self.db = self.client[config("mydb")]
            self.col = self.db[config("mycol")]
            logger.info("Connection Successfull")

        except Exception:
            logger.exception("Connection Unsuccessfull")
            
    def insert_record(self):
        try:
            if os.path.isfile("../CosmosDB/MongoDB/resources/book.json"):
                with open("../CosmosDB/MongoDB/resources/book.json", "r") as f:
                    entries = json.load(f)
                    listBooks = entries["book"]
                    self.col.insert_many(listBooks)
                    logger.info("Insertion Successfull")
        except Exception:
            logger.exception("Insertion Unsuccessfull")

    def update_record(self):
        try:
            search = {"bookid": "004"}
            setting = {"$set": {"type": "Fiction"}}
            self.col.update_one(search, setting)
            logger.info("Update Successfull")
        except Exception:
            logger.exception("Update Unsuccessful")

    def read_collection(self):
        try:
            logger.info("Printing Records")
            for b in self.col.find():
                logger.info(b)
        except Exception:
            logger.exception("Data Unread")

    def drop_collection(self):
        try:
            self.col.drop() 
            logger.info("Drop Collection Successfull")
        except Exception:
            logger.exception("Drop Collection Unsuccessfull")

    def delete_record(self):
        try:
            search = {"_id": 5}
            self.col.delete_one(search)
            logger.info("Delete Record Successfull")
        except Exception:
            logger.exception("Delete Record Unsuccessfull")


if __name__ == "__main__":
    obj = mongoDB()
    obj.insert_record()
    obj.update_record()
    obj.delete_record()
    obj.read_collection()
    obj.drop_collection()