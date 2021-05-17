'''
@Author: Sankar
@Date: 2021-05-17 14:52:25
@Last Modified by: Sankar
@Last Modified time: 2021-05-17 17:10:09
@Title : Basic CRUD Operation in SQL API
'''
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from decouple import config
from log import logger
import json

class CRUD:
    '''
    Class:
        CRUD
    Description:
        Class to perform Basic CRUD Operations on Cosmos DB SQL API
    Functions:
        dbConnection()
        create_database()
        create_container()
        insert_record()
        delete_record()
        read_record()
        drop_database()
    Variable:
        None
    '''
    def __init__(self):
        self.url = config('ACCOUNT_URI')
        self.key = config('ACCOUNT_KEY')
        self.clientConnection()
        self.create_database()
        self.create_container()

    def clientConnection(self):
        '''
        Description:
            Connection to Azure Cosmos DB
        Parameter:
            None
        Return:
            None
        '''
        try:
            self.client = CosmosClient(self.url, credential = self.key)
            logger.info("Connection Successfull")
        
        except Exception:
            logger.exception("Connection Unsuccessfull")

    def create_database(self):
        '''
        Description:
            Create a Database
        Parameter:
            None
        Return:
            None
        '''
        try: 
            database_name = 'testDatabase'
            self.mydb = self.client.create_database(database_name)
            logger.info("Database Creation Successfull")
        except exceptions.CosmosResourceExistsError:
            self.mydb = self.client.get_database_client(database_name)
            logger.warning("Database Exists")
        except Exception:
            logger.exception("Database Creation Unsuccessfull")

    def create_container(self):
        '''
        Description:
            Create a Container
        Parameter:
            None
        Return:
            None
        '''
        try: 
            container_name = 'products'
            self.myContainer = self.mydb.create_container(id = container_name, partition_key = PartitionKey(path="/productName"))
            logger.info("Container Creation Successfull")
        except exceptions.CosmosResourceExistsError:
            self.myContainer = self.mydb.get_container_client(container_name)
            logger.warning("Container Exists")
        except Exception:
            logger.exception("Container Creation Unsuccessfull")

    def insert_record(self):
        '''
        Description:
            Insert Records to the container
        Parameter:
            None
        Return:
            None
        '''
        try:
            for i in range(1, 10):
                self.myContainer.upsert_item(
                    {
                    'id': 'item{0}'.format(i),
                    'productName': 'Widget',
                    'productModel': 'Model {0}'.format(i)
                    }
                )
            logger.info("Insert Record Successfull")
        except Exception:
            logger.exception("Insert Record Unsuccessfull")

    def delete_record(self):
        '''
        Description:
            Delete a record in the container
        Parameter:
            None
        Return:
            None
        '''
        try:
            myQuery = "SELECT * FROM products p WHERE p.id = 'item3'"
            for item in self.myContainer.query_items(query = myQuery, enable_cross_partition_query = True):
                self.myContainer.delete_item(item, partition_key = 'Widget')
            logger.info("Delete Record Successfull")
        except Exception:
            logger.exception("Delete Record Unsuccessfull")

    def read_record(self):
        '''
        Description:
            Read a record from the container
        Parameter:
            None
        Return:
            None
        '''
        try:
            myQuery = "SELECT * FROM products p WHERE p.productModel = 'Model 2'"
            for item in self.myContainer.query_items(query = myQuery, enable_cross_partition_query = True):
                print(json.dumps(item, indent = True))
            logger.info("Read Record Successfull")
        except Exception:
            logger.exception("Read Record Unsuccessfull")
    
    def drop_database(self):
        '''
        Description:
            Delete the created Database
        Parameter:
            None
        Return:
            None
        '''
        try:
            self.client.delete_database("testDatabase")
            logger.info("Database Drop Successfull")
        except Exception:
            logger.exception("Drop Database Unsuccessfull")

if __name__ == "__main__":
    obj = CRUD()
    obj.insert_record()
    obj.delete_record()
    obj.read_record()
    obj.drop_database()