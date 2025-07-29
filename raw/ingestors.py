import datetime
import requests
import json
import yaml
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

    def get_data(self):
        pass

class APIIngestor(RawIngestor):
    def __init__(self, table_name, dbfs_path, schema, url, endpoint, headers, results='stocks'):
        super().__init__(table_name, dbfs_path, schema)
        self.url = url
        self.endpoint = endpoint
        self.headers = headers       
        self.results = results 
    
    def get_data(self):   
        url_path = f"{self.url}/{self.endpoint}"
        try:
            response = requests.get(url_path, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                data['loaded_at'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data['asset_type'] = self.results
                return data
            else:
                print(f"Error: {response.status_code}")
        except Exception as e:
                print(e)
    





