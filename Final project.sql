-- Databricks notebook source
-- MAGIC %md
-- MAGIC DS3002 Capstone project
-- MAGIC the dimensional data mart extracts data from a public github API and kaggle datasets, which is then transformed and loaded into connected databases. The following data is read in from a local file using pandas.
-- MAGIC The data is obtained from kaggle and Yahoo Finance, 
-- MAGIC and includes general financial information on 2310 ETFs, which can be compared by
-- MAGIC differences in sizes, capitalization and returns.
-- MAGIC This console will read in the data and give the user a summary of 
-- MAGIC information from the dataset.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC install pymongo[srv]
-- MAGIC import pymongo
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, TimeStampType, BinaryType

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC import json
-- MAGIC import pyspark.pandas as pd
-- MAGIC 
-- MAGIC from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType
-- MAGIC import csv
-- MAGIC import requests
-- MAGIC import requests.exceptions
-- MAGIC from sqlalchemy import create_engine

-- COMMAND ----------

-- MAGIC %md
-- MAGIC this datafile used is from a Kaggle dataset that focuses on ETFs using Yahoo Finance   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC data_file=r"C:\ETFs.csv"

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC establish connections with databases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jdbcHostname="ds3002adisgen2.database.windows.net"
-- MAGIC jdbcDatabase="data.mysql"
-- MAGIC jdbcPort=1433
-- MAGIC jdbcUrl="jdbc:sqlserver://:{0}:{1};database={2}".format(jdbcHostname,jdbcPort, jdbcDatabase)
-- MAGIC 
-- MAGIC connectionProperties={
-- MAGIC "user" : "student",
-- MAGIC "password" : "Passw0rd123",
-- MAGIC "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
-- MAGIC 
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC data_file = r"C:\ETFs.csv"
-- MAGIC 
-- MAGIC host_name = "localhost"
-- MAGIC ports = {"mysql" : 3306}
-- MAGIC 
-- MAGIC user_id = "root"
-- MAGIC pwd = "meimei"
-- MAGIC 
-- MAGIC src_dbname = "data_cleaning"
-- MAGIC dst_dbname = "ETFs"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC MySQL connection and create database

-- COMMAND ----------

-- MAGIC %python
-- MAGIC conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}"
-- MAGIC sqlEngine = create_engine(conn_str, pool_recycle=3600)
-- MAGIC 
-- MAGIC sqlEngine.execute(f"DROP DATABASE IF EXISTS `{dst_dbname}`;")
-- MAGIC sqlEngine.execute(f"CREATE DATABASE `{dst_dbname}`;")
-- MAGIC sqlEngine.execute(f"USE {dst_dbname};")
-- MAGIC 
-- MAGIC db_name = "ETFs"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a fact table from data of ETFs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/ETFs.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "false"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC temp_table_name = "ETFs_csv"
-- MAGIC 
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------


select * from `ETFs_csv`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC permanent_table_name = "ETFs"
-- MAGIC 
-- MAGIC df.write.format("parquet").saveAsTable(permanent_table_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataframe = df
-- MAGIC table_name = 'ETFs'
-- MAGIC primary_key = 'fund_symbol'
-- MAGIC db_operation = 'insert'
-- MAGIC dst_dbname='data_cleaning'
-- MAGIC 
-- MAGIC set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Conect to MongoDB

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC host_name = "localhost"
-- MAGIC ports = {"mongo" : 27017, "mysql" : 3306}
-- MAGIC 
-- MAGIC user_id = "root" 
-- MAGIC pwd = "Passw0rd123!" 
-- MAGIC 
-- MAGIC src_dbname = "data_cleaning"
-- MAGIC dst_dbname = "ETFs"
-- MAGIC 
-- MAGIC conn_str = f"mongodb://{host_name}:{ports['mongo']}/"
-- MAGIC client = pymongo.MongoClient(conn_str)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC src_dbname = "ETFs"
-- MAGIC ports = {"mongo" : 27017, "mysql" : 3306}
-- MAGIC port = ports["mongo"]
-- MAGIC conn_str = f"mongodb://{host_name}:{port}/"
-- MAGIC client = pymongo.MongoClient(conn_str)
-- MAGIC db = client[src_dbname]
-- MAGIC 
-- MAGIC data_dir = os.path.join(os.getcwd())
-- MAGIC 
-- MAGIC json_files = {"ETFs" : 'ETFs.json',
-- MAGIC              }
-- MAGIC 
-- MAGIC for file in json_files:
-- MAGIC     json_file = os.path.join(data_dir, json_files[file])
-- MAGIC     with open(json_file, 'r') as openfile:
-- MAGIC         json_object = json.load(openfile)
-- MAGIC         file = db[file]
-- MAGIC         result = file.insert_many(json_object)
-- MAGIC         
-- MAGIC client.close()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC upload json to mongodb servers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def set_mongo_collection (user_id, pwd, cluster_name, db_name, src_file_path, json_files)
-- MAGIC     mongo_uri=f"mongodb+srv://{user_id}:pwd@{cluster_name}.zibbf.mongodb.net/{db_name}?retryWrites=true&w=majority"
-- MAGIC     client=pymongo.MongoClient(mongo_uri)
-- MAGIC     db=client[db_name]
-- MAGIC     
-- MAGIC '''Read in json file and create new collection'''
-- MAGIC for file in json_files:
-- MAGIC     db.drop_collection(file)
-- MAGIC     json_file=os.path.join(src_file_path, json_files[file])
-- MAGIC     with open(json_file, 'r') as openfile:
-- MAGIC         json_object=json.load(openfile)
-- MAGIC         file=db[file]
-- MAGIC         result=file.insert_many(json_object)
-- MAGIC         
-- MAGIC client.close()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC define functions and get dataframes with SQL and mongo DB

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC def get_sql_dataframe(user_id, pwd, host_name, db_name, sql_query):
-- MAGIC     # Create a connection to the MySQL database
-- MAGIC     conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
-- MAGIC     sqlEngine = create_engine(conn_str, pool_recycle=3600)
-- MAGIC 
-- MAGIC     conn = sqlEngine.connect()
-- MAGIC     dframe = pd.read_sql(sql_query, conn);
-- MAGIC     conn.close()
-- MAGIC     
-- MAGIC     return dframe
-- MAGIC 
-- MAGIC def set_dataframe(user_id, pwd, host_name, db_name, df, table_name, pk_column, db_operation):
-- MAGIC     # Create a connection to the MySQL database
-- MAGIC     conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
-- MAGIC     sqlEngine = create_engine(conn_str, pool_recycle=3600)
-- MAGIC     connection = sqlEngine.connect()
-- MAGIC 
-- MAGIC     if db_operation == "insert":
-- MAGIC         df.to_sql(table_name, con=connection, index=False, if_exists='replace')
-- MAGIC         sqlEngine.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});")
-- MAGIC             
-- MAGIC     elif db_operation == "update":
-- MAGIC         df.to_sql(table_name, con=connection, index=False, if_exists='append')
-- MAGIC     
-- MAGIC     connection.close()
-- MAGIC     
-- MAGIC conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}"
-- MAGIC sqlEngine = create_engine(conn_str, pool_recycle=3600)
-- MAGIC 
-- MAGIC sqlEngine.execute(f"DROP DATABASE IF EXISTS `{dst_dbname}`;")
-- MAGIC sqlEngine.execute(f"CREATE DATABASE `{dst_dbname}`;")
-- MAGIC sqlEngine.execute(f"USE {dst_dbname};")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Catch errors if any 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC conn = pymysql.connect(host=host_name, user=user_id, password=pwd, database=src_dbname)
-- MAGIC cursor = conn.cursor()
-- MAGIC 
-- MAGIC try:
-- MAGIC     cursor.execute('SELECT * FROM fund_type;')
-- MAGIC     
-- MAGIC     for row in cursor.fetchmany(size=2):
-- MAGIC         print(row)
-- MAGIC         
-- MAGIC     cursor.close()
-- MAGIC     
-- MAGIC except:
-- MAGIC     print ("Error: unable to fetch data")
-- MAGIC     
-- MAGIC conn.close()
-- MAGIC 
-- MAGIC conn = pymysql.connect(host=host_name, user=user_id, password=pwd, database=src_dbname)
-- MAGIC cursor = conn.cursor(pymysql.cursors.DictCursor)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Convert the general format and data structure of the data source. 
-- MAGIC CSV to JSON

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC def csv_to_json(csvFilePath, jsonFilePath):
-- MAGIC     jsonArray = []
-- MAGIC       
-- MAGIC     #read csv file
-- MAGIC     with open(csvFilePath, encoding='utf-8') as csvf: 
-- MAGIC         #load csv file data using csv library's dictionary reader
-- MAGIC         csvReader = csv.DictReader(csvf) 
-- MAGIC 
-- MAGIC         #convert each csv row into python dict
-- MAGIC         for row in csvReader: 
-- MAGIC             #add this python dict to json array
-- MAGIC             jsonArray.append(row)
-- MAGIC   
-- MAGIC     #convert python jsonArray to JSON String and write to file
-- MAGIC     with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
-- MAGIC         jsonString = json.dumps(jsonArray, indent=4)
-- MAGIC         jsonf.write(jsonString)
-- MAGIC         

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Integrating an API from reddit that updates every 15 minutes. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def get_api_response(url, response_type):
-- MAGIC     try:
-- MAGIC         response = requests.get('https://tradestie.com/api/v1/apps/reddit')
-- MAGIC         response.raise_for_status()
-- MAGIC     
-- MAGIC     except requests.exceptions.HTTPError as errh:
-- MAGIC         return "An Http Error occurred: " + repr(errh)
-- MAGIC     

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Transforming the data. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC 
-- MAGIC data_file =r"C:\ETFs.csv"
-- MAGIC df = pd.read_csv(data_file, header=0, index_col=0)
-- MAGIC cols = ['fund_category', 'avg_vol_3month', 'week52_high_low_change',
-- MAGIC  'fund_yield', 'fund_return_ytd']
-- MAGIC 
-- MAGIC new_data = df[cols]
-- MAGIC 
-- MAGIC x1=len(new_data.index)
-- MAGIC y2=len(new_data.columns)
-- MAGIC 
-- MAGIC print(" the number of records is "  + str (x1) + " and the number of columns is " +  str (y2))   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Making dimension table 1 for exchanges by type of investment. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC user_id="root"
-- MAGIC pwd="meimei"
-- MAGIC host_name="localhost"
-- MAGIC src_dbname = "data_cleaning"
-- MAGIC sql_exchange = "SELECT * FROM data_cleaning.exchange_name;"
-- MAGIC df_exchange = get_sql_dataframe(user_id, pwd, host_name, src_dbname, sql_exchange)
-- MAGIC 
-- MAGIC 
-- MAGIC df_exchange.rename(columns={
-- MAGIC     "exchange_name":"name",
-- MAGIC     "investment_type":"type",
-- MAGIC     "currency":"currency",
-- MAGIC     "region":"region",
-- MAGIC     
-- MAGIC     }, inplace=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC src_dbname = "ETFs"
-- MAGIC sql_date = "SELECT * FROM ETFs.exchange_timezone;"
-- MAGIC df_date= get_sql_dataframe(user_id, pwd, host_name, src_dbname, sql_exchange)
-- MAGIC 
-- MAGIC df_exchange['Date'] = pd.to_datetime(df["exchange_timezone"].dt.strftime('%Y-%m-%d %H:%M:%S'))
-- MAGIC 
-- MAGIC df_date.rename(columns={
-- MAGIC     "exchange_timezone":"date",
-- MAGIC     "fund_symbol":"symbol",
-- MAGIC     "size_type":"size"
-- MAGIC     }, inplace=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cols = df['_c1, '_c6','_c10', '_c12']
-- MAGIC 
-- MAGIC new_data = df[cols]
-- MAGIC 
-- MAGIC '''
-- MAGIC create key
-- MAGIC '''
-- MAGIC new_data.rename(columns={"_c1":"etf_key"}, inplace=True)
-- MAGIC 
-- MAGIC '''
-- MAGIC new table created
-- MAGIC '''
-- MAGIC initial_value = 1
-- MAGIC new_data.insert(loc=0, column='etf_key', value=range(initial_value, len(new_data) +initial_value))
-- MAGIC 
-- MAGIC dataframe = new_data
-- MAGIC table_name = 'new_data_table'
-- MAGIC primary_key = 'etf_key'
-- MAGIC db_operation = "insert"
-- MAGIC 
-- MAGIC set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC API data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df.rename(columns={
-- MAGIC     "sentiment":"sentiment",
-- MAGIC     "sentiment_score":"sentiment_score",
-- MAGIC     "no_of_comments":"no_of_comments",
-- MAGIC     "ticker":"ticker",
-- MAGIC     }, inplace=True)
-- MAGIC 
-- MAGIC dataframe = df_etfs
-- MAGIC table_name = 'dim_etfs'
-- MAGIC primary_key = 'ticker'
-- MAGIC db_operation = "insert"
-- MAGIC 
-- MAGIC set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC export back to excel 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC excel_writer = "C:\ETFs.csv"
-- MAGIC # df_fact_collections.to_excel(excel_writer)
