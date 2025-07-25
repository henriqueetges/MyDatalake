import datetime
import requests
import json
import os
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class RawIngestor:

    def __init__(self, table_name, dbfs_path, schema):
        self.table_name = table_name
        self.dbfs_path = dbfs_path
        self.schema = schema
    
    def save_data(self, data):
        ts = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        data = json.dumps(data)
        file_path = f"{self.dbfs_path}/{self.schema}/{self.table_name}/{self.table_name}_{ts}.json"
        dbutils.fs.put(file_path, data, overwrite=True)

    def run(self):
        data = self.get_data()
        self.save_data(data)

class APIIngestor(RawIngestor):

    def __init__(self, table_name, dbfs_path, schema, url, endpoint, headers, params=''):
        super().__init__(table_name, dbfs_path, schema)
        self.url = url
        self.endpoint = endpoint
        self.headers = headers
        self.params = params

    def get_data(self, **kwargs):        
        url_path = f"{self.url}/{self.endpoint}?{self.params}"
        try:
            response = requests.get(url_path, headers=self.headers, params=self.params)
        except Exception as e:
                print(e)
        return response.json()
    
    def run(self):
        data = self.get_data()
        self.save_data(data)

