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

class MongoDB:
    '''
    Class:
        MongoDB
    Description:
        Class to perform Basic CRUD Operations on Cosmos DB MongoDB API
    Functions:
        dbConnection()
        insert_record()
        update_record()
        delete_record()
        read_collection()
        drop_collection()
    Variable:
        None
    '''
    def __init__(self):
        self.conString = config('conString')
        self.dbConnection()

    def dbConnection(self):
        '''
        Description:
            Connection to Azure Cosmos DB
        Parameter:
            None
        Return:
            None
        '''
        try:
            self.client = pymongo.MongoClient(self.conString)
            self.db = self.client[config("mydb")]
            self.col = self.db[config("mycol")]
            logger.info("Connection Successfull")

        except Exception:
            logger.exception("Connection Unsuccessfull")
            
    def insert_record(self):
        '''
        Description:
            Insert Reccords from a json file
        Parameter:
            None
        Return:
            None
        '''
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
        '''
        Description:
            Update a record in collections book
        Parameter:
            None
        Return:
            None
        '''
        try:
            search = {"bookid": "004"}
            setting = {"$set": {"type": "Fiction"}}
            self.col.update_one(search, setting)
            logger.info("Update Successfull")
        except Exception:
            logger.exception("Update Unsuccessful")

    def read_collection(self):
        '''
        Description:
            Read from collections and print the  records
        Parameter:
            None
        Return:
            None
        '''
        try:
            logger.info("Printing Records")
            for b in self.col.find():
                print(b)
        except Exception:
            logger.exception("Data Unread")

    def delete_record(self):
        '''
        Description:
            Delete a record in Collection book
        Parameter:
            None
        Return:
            None
        '''
        try:
            search = {"_id": 5}
            self.col.delete_one(search)
            logger.info("Delete Record Successfull")
        except Exception:
            logger.exception("Delete Record Unsuccessfull")

    def drop_collection(self):
        '''
        Description:
            Drop the created collection in Azure Cosmos DB
        Parameter:
            None
        Return:
            None
        '''
        try:
            self.col.drop() 
            logger.info("Drop Collection Successfull")
        except Exception:
            logger.exception("Drop Collection Unsuccessfull")

if __name__ == "__main__":
    obj = MongoDB()
    obj.insert_record()
    obj.update_record()
    obj.delete_record()
    obj.read_collection()
    obj.drop_collection()