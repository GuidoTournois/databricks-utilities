# Databricks notebook source
from pyspark.sql.functions import *

class dataloader_azure_active_directory:
  def __init__(self, URL, database, username, password):
    
    self.jdbcUrl = ('jdbc:sqlserver://' + URL+ ':1433;database=' + database+ ';encrypt=true;trustServerCertificate=true' + 
                    ';hostNameInCertificate=*.database.windows.net;authentication=ActiveDirectoryPassword')

    self.connectionProperties = {
      "user" : username,
      "password" : password,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
  def load_table(self, table_name=None, query=None):
    '''Read table or execute sql statement on database'''
    if table_name:
      print('reading table')
      return spark.read.jdbc(self.jdbcUrl, table= table_name , properties = self.connectionProperties)    
    elif query:
      print('reading query')
      return spark.read.jdbc(self.jdbcUrl, table= query , properties = self.connectionProperties)    
    else: 
      print('Please provide table name or query in the form: "(select ... from ...) alias_name"')
  
  def write_table(self, df, table_name, mode='append'):
    df.write.jdbc(self.jdbcUrl, table=table_name, mode=mode, properties = self.connectionProperties)
    return True


# COMMAND ----------

class dataloader_SQL_user:
  ''' This class uses distributed loading and saving using polybase.'''
  def __init__(self, URL, database, username, password):
    self.username = username
    self.password = password
    self.jdbcUrl = ('jdbc:sqlserver://' + URL+ ':1433;database=' + database+ ';')
    spark.conf.set(dbutils.secrets.get(scope = "blob-storage", key = "account"), dbutils.secrets.get(scope = "blob-storage", key = "key")) 
  
  def load_table(self, table_name):
    df =  (spark.read.format("com.databricks.spark.sqldw")
                    .option("url", self.jdbcUrl)
                    .option("user", self.username)
                    .option("password", self.password)
                    .option("tempDir", dbutils.secrets.get(scope = "blob-storage", key = "tmpDir"))
                    .option("forward_spark_azure_storage_credentials", "true")
                    .option("dbTable", table_name)
                    .load())
    return df

  def write_table(self, df, table_name, mode):
      (df.write.mode(mode).format("com.databricks.spark.sqldw")
          .option("url", self.jdbcUrl)
          .option("forwardSparkAzureStorageCredentials", "true")
          .option("dbTable", table_name)
          .option("tempDir", dbutils.secrets.get(scope = "blob-storage", key = "tmpDir"))
          .option("user", self.username)
          .option("password", self.password)
          .save())