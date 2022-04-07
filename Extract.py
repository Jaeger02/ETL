import boto3
import pandas as pd
from io import StringIO, BytesIO
import os, glob
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib import pyplot
import numpy as np
from pyparsing import col
class Parametros:
    def __init__(self,arg_date,src_format,src_bucket,trg_bucket,columns,key): 
      self.__arg_date=arg_date
      self.__src_format=src_format
      self.__src_bucket=src_bucket
      self.__trg_bucket=trg_bucket
      self.__columns=columns
      self.__key=key

    def arg_date(self): 
        return self.__arg_date
    def src_format(self): 
        return self.__src_format
    def src_bucket(self): 
        return self.__src_bucket
    def trg_bucket(self): 
        return self.__trg_bucket
    def columns(self): 
        return self.__columns
    def key(self): 
        return self.__key
    

class Init(Parametros):
    def __init__(self,bucket): 
      self.__bucket=bucket
    def bucket(self): 
        return self.__bucket

class Extract:
    
    def read_csv_to_df(self,filename, bucket):
        csv_obj = bucket.Object(key=filename).get().get('Body').read().decode('utf-8')
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=',')
        return df
    def return_objects(self,src_bucket, src_format,arg_date, bucket):
        arg_date_dt = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
        objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], src_format).date() >= arg_date_dt]
        ##objKey= obj.key
        return objects
    def extract(self,objects, bucket, columns, key):
        df_all = pd.concat([self.read_csv_to_df(obj.key, bucket) for obj in objects], ignore_index=True)
        df_all = df_all.loc[:, columns]
        df_all.dropna(inplace=True)
        return df_all
    
