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
        df = spark.createDataFrame(data)
        file_path = f"{self.dbfs_path}/{self.schema}/{self.table_name}/{self.table_name}_{ts}"
        df.write.parquet(file_path)        

    def run(self):
        data = self.get_data()
        self.save_data(data)

class APIIngestor(RawIngestor):
    def __init__(self, table_name, dbfs_path, schema, url, endpoint, headers, **kwargs):
        super().__init__(table_name, dbfs_path, schema)
        self.url = url
        self.endpoint = endpoint
        self.headers = headers        
        for k, v in kwargs.items():
            setattr(self, k, v)

    def get_data(self, **kwargs):   
        params = kwargs.get('params', {})     
        url_path = f"{self.url}/{self.endpoint}"
        try:
            response = requests.get(url_path, headers=self.headers, params=params)
        except Exception as e:
                print(e)
        return response.json()
    
    def run(self):
        data = self.get_data()
        self.save_data(data)

