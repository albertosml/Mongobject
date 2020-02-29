from pymongo import MongoClient
from pymongo import ReturnDocument
from enum import Enum

class Mongobject(object):
    """
    Mongobject contains a object that connects to a MongoDB database through pymongo library 
    """

    def __init__(self, uri=None, host=None, port=None, database=None, collection=None):
        """
        Initialize function, that creates the MongoDB connection and, also saves database and collection names

        :param string uri: MongoDB URI
        :param string host: MongoDB database hostname 
        :param int port: MongoDB database port
        :param string database: Database name
        :param string collection: Collection name  
        """
        # Create connection to Mongo Instance
        self._connect_database(uri=uri, host=host, port=port)

        # Save database name
        if database:
            self.database = database

        # Save collection name
        if collection:
            self.collection = collection

        # Establish Find Options
        FIND = 1
        FIND_ONE = 2
        FIND_ONE_AND_DELETE = 3
        FIND_ONE_AND_REPLACE = 4
        FIND_ONE_AND_UPDATE = 5

    def __del__(self):
        """
        Method that closes MongoDB connection when object is going to be destroyed
        """

        # Close connection
        if self.instance:
            self.instance.close()

    def _server_info(self):
        """
        Method that gives information about MongoDB server

        :return Information about MongoDB server
        """
        return self.instance.server_info()

    def get_info(self):
        """
        Method that returns Mongobject information

        :return Mongobject with instance information
        """
        return {
            'host': self._server_info(),
            'database': self.database,
            'collection': self.collection
        }

    def _connect_database(self, uri=None, host=None, port=None):
        """
        Method that creates an instance with the MongoDB connection

        :param string uri: MongoDB URI
        :param string host: MongoDB database hostname
        :param int port: MongoDB database port
        """

        # Check parameters and create MongoDB connection
        if uri is not None and uri.startswith('mongodb://'):
            self.instance = MongoClient(uri, ssl=True)
        elif host and port:
            self.instance = MongoClient(host, port, ssl=True)
        elif host or uri:
            self.instance = MongoClient('mongodb://' + host + '/?ssl=true')
        else:
            self.instance = MongoClient(ssl=True)

    def _check_database(self):
        """
        Method that checks if database is in MongoDB instance

        :return true if database exists in instance, false otherwise
        """
        return self.database in self.instance.keys()

    def _check_collection(self):
        """
        Method that checks if database is in MongoDB instance and collection exists in
        connected database, at this moment

        :return true if collection exists in database, false otherwise
        """
        return self._check_database() and self.collection in self.instance[self.database].keys()

    def set_instance(self, uri=None, host=None, port=None):
        """
        Method that modifies the host of MongoDB instance

        :param string uri: MongoDB URI
        :param string host: MongoDB database hostname
        :param int port: MongoDB database port
        """
        # Close old connection
        if self.instance:
            self.instance.close()

        # Open new connection
        self._connect_database(uri=uri, host=host, port=port)

    def get_instance(self):
        """
        Method that obtains MongoDB instance

        :return MongoDB instance
        """
        return self.instance

    def set_database(self, database):
        """
        Method that modifies database name

        :param string database: MongoDB database name
        """
        if database:
            self.database = database

    def get_database(self):
        """
        Method that obtains MongoDB database name

        :return MongoDB database name
        """
        return self.database

    def set_collection(self, collection):
        """
        Method that modifies collection name

        :param string database: MongoDB collection name
        """
        if collection:
            self.collection = collection

    def get_collection(self):
        """
        Method that obtains MongoDB collection name

        :return MongoDB collection name
        """
        return self.collection

    def get_available_databases(self, filter=None):
        """
        Method that obtains all database names of MongoDB instance

        :param string filter: Filter for searching databases
        :return List of databases of MongoDB instance
        """
        return self.instance.list_database_names() if self.instance else []

    def get_available_collections(self, filter=None):
        """
        Method that obtains all (filtered) collections of MongoDB database

        :param string filter: Filter for searching databases
        :return List of filtered collections of MongoDB database
        """
        return self.instance[self.database].list_collection_names(filter=filter) if self._check_database() else []

    def create_database(self, db_name=None):
        """
        Method that creates a database in the MongoDB instance

        :param string db_name: Database name to create
        """
        if self.instance and db_name and db_name not in self.instance:
            self.instance[db_name] = {}
            self.set_database(db_name)

    def drop_database(self):
        """
        Method that drops a MongoDB database
        """
        if self._check_database():
            # Specified database name in the class will be dropped
            self.instance.drop_database(self.database)

            # Reset database and collection name
            self.set_database(None)
            self.set_collection(None)

    def create_collection(self, collection_name=None):
        """
        Method that creates a collection in the MongoDB database

        :param string db_name: Collection name to create
        """
        if self._check_database():
            # Create collection
            self.instance[self.database].create_collection(collection_name)

            # Set collection name into object
            self.set_collection(collection_name)

    def drop_collection(self):
        """
        Method that drops a MongoDB collection
        """
        if self._check_collection():
            # Specified collection name in the class will be dropped
            self.instance[self.database].drop_collection(self.collection)

            # Reset collection name
            self.set_collection(None)

    def insert(self, data=None):
        """
        Method that inserts data into MongoDB collection

        :param list|dict data: Data to insert into MongoDB collection
        :return true if data have been inserted successfully, otherwise, this method will return false
        """
        # Check Mongoobject and parameter data
        if data and self._check_collection():
            # List case
            if isinstance(data, list) and len(data) > 1:
                result = self.instance[self.database][self.collection].insert_many(data)
                return len(result.inserted_ids) == len(data)
            # Dict case
            elif isinstance(data, dict):
                result = self.instance[self.database][self.collection].insert_one(data)
                return result.inserted_id is not None
            # Insert the unique item of the list
            else:
                result = self.instance[self.database][self.collection].insert_one(data[0])
                return result.inserted_id is not None
        else:
            return False

    def replace(self, filter={}, replacement={}, upsert=False, only_one=False):
        """
        Method that replaces all documents matching the filter

        :param dict filter: Query for taking documents to replace
        :param dict replacement: Replacement document
        :param bool upsert: If there wouldn't be any matching document, replacement document would be inserted
        :param bool only_one: Replace only one matching document
        :return true if replacement has been executed succesfully, false if there has been a problem during execution
        """
        if self._check_collection():
            if only_one:
                result = self.instance[self.database][self.collection].replace_one(filter, replacement, upsert)
                return result.modified_count == 1
            else:
                # While there are documents to replace, replace_one method will be executed
                while True:
                    result = self.instance[self.database][self.collection].replace_one(filter, replacement, upsert)

                    if result.matched_count == result.modified_count:
                        return True
                    elif result.modified_count == 0: # Error during replacing (Example: 5 matched documents, 0 modified documents)
                        return False
        else:
            return False

    def update(self, filter={}, update={}, upsert=False, only_one=False):
        """
        Method that updates all documents matching the filter

        :param dict filter: Query for taking documents to update
        :param dict update: Modifications to do in the update
        :param bool upsert: If there wouldn't be any matching document, document with modifications would be inserted
        :param bool only_one: Update only one matching document
        :return true if updating has been executed succesfully, false if there has been a problem during execution
        """
        if self._check_collection():
            if only_one:
                result = self.instance[self.database][self.collection].update_one(filter, update, upsert)
                return result.modified_count == 1
            else:
                result = self.instance[self.database][self.collection].update_many(filter, update, upsert)
                return result.modified_count == result.matched_count
        else:
            return False

    def delete(self, filter={}, only_one=False):
        """
        Method that deletes all documents matching the filter

        :param dict filter: Query for taking documents to delete
        :param bool only_one: Delete only one matching document
        :return true if erasing has been executed succesfully, false if there has been a problem during execution
        """
        if self._check_collection():
            if only_one:
                result = self.instance[self.database][self.collection].delete_one(filter)
            else:
                result = self.instance[self.database][self.collection].delete_many(filter)

            return result.deleted_count > 0
        else:
            return False

    def find(self, filter={}, projection=None, skip=0, limit=0, sort=None, find_option=False, document={}, upsert=False, return_document_before=True):
        """
        Method that deletes all documents matching the filter

        :param dict filter: Query for taking documents to find
        :param dict projection: List with fields that result should return or not
        :param int skip: Number of documents to omit from result set start
        :param int limit: Maximum number of documents to retrieve, 0 for no limit
        :param list sort: Specification of sort order for this operation
        :param find_option int: Selected option for finding
        :param dict document: Replacement or updating document
        :param bool upsert: If there wouldn't be any matching document, replacement document would be inserted
        :param bool return_document_before: Return original or replaced/inserted document
        :return dict with results { 'id1': { ... }, 'id2': { ... } }
        """
        if self._check_collection():
            # Check find option
            if find_option == Mongobject.FIND:
                result = self.instance[self.database][self.collection].find(filter, projection=projection, skip=skip, limit=limit, sort=sort)
            elif find_option == Mongobject.FIND_ONE:
                result = self.instance[self.database][self.collection].find_one(filter, projection=projection, skip=skip, sort=sort)
            elif find_option == Mongobject.FIND_ONE_AND_DELETE:
                result = self.instance[self.database][self.collection].find_one_and_delete(filter, projection=projection, skip=skip, sort=sort)
            elif find_option == Mongobject.FIND_ONE_AND_REPLACE:
                result = self.instance[self.database][self.collection].find_one_and_replace(filter, replacement=document, upsert=upsert, projection=projection, skip=skip, sort=sort, return_document=ReturnDocument.BEFORE if return_document_before else ReturnDocument.AFTER)
            elif find_option == Mongobject.FIND_ONE_AND_UPDATE:
                result = self.instance[self.database][self.collection].find_one_and_update(filter, update=document, upsert=upsert, projection=projection, skip=skip, sort=sort, return_document=ReturnDocument.BEFORE if return_document_before else ReturnDocument.AFTER)
            else:
                return {}

            # Process result
            find_result = {}

            for res in result:
                dic = res
                del dic['_id']
                find_result[res.get('_id')] = dic

            return find_result
        else:
            return {}

    def count(self, filter={}, skip=0, limit=0):
        """
        Method that counts all documents matching the filter

        :param dict filter: Query for taking documents to find
        :param int skip: Number of documents to omit from result set start
        :param int limit: Maximum number of documents to retrieve, 0 for no limit
        :return Number of documents that matches with filter, -1 indicates an error while counting
        """
        if self._check_collection():
            return self.instance[self.database][self.collection].count_documents(filter=filter, skip=skip, limit=limit)
        else:
            return -1

    def distinct(self, key, filter=None):
        """
        Method for obtaining distinct values for key in the collection

        :param string key: Field name for obtaining distinct values
        :param dict filter: Query for retrieving distinct values
        :return List of distinct values for this collection
        """
        if self._check_collection():
            return self.instance[self.database][self.collection].distinct(key, filter=filter)
        else:
            return []
