# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
target="dbfs:/mnt/destination/"
print(target)
target= target.split("/",2)
target=target[2]
target=target.split("/")
target=target[0]
print(target)
import pandas as pd

columns = StructType([StructField('FileName',
                                  StringType(), True),
                    StructField('FolderName',
                                StringType(), True),
                    StructField('FilePath',
                                StringType(), True),
                    StructField('FileType',
                                StringType(), True),
                    StructField('FileColumns',
                                StringType(), True),
                    StructField('FileDelimiter',
                                StringType(), True),
                    StructField('JobStartDateTime',
                                StringType(), True),
                    StructField('JobEndDateTime',
                                StringType(), True),
                    StructField('TargetTable',
                                StringType(), True)])

 
df=spark.createDataFrame(data=[], schema=columns)
display(df)
df.write.format("com.crealytics.spark.excel").option("header","true").mode("overwrite").save("dbfs:/mnt/source/Vehicle_Data/metadata.csv")

# COMMAND ----------

from re import match
import re
from pyspark.sql.functions import col,lit,when
from datetime import datetime
import pytz
import pandas as pd
matched_files=[]
files=dbutils.fs.ls('/mnt/destination/')
source_files=dbutils.fs.ls('/mnt/source/Vehicle_Data/')
for file in files:
    #print(file)
    if(match(".*csv", file.name)): 
        matched_files.append(file.path)
        print("Metadata File  found, updating  metadata file")
metadata_file=matched_files
#print(metadata_file)
if not metadata_file:
    print("Metadata is not avaliable, creating first metadata file")
    for name in source_files:
        if(match(".*csv", name.name)):
            print("csv files are there in Source")
            CSV= spark.read.format("csv").option("header",True).load('/mnt/source/Vehicle_Data/'+name.name)
            #display(CSV)
            File_name=name.name
            File_name1=File_name.split(".")
            File_name1=File_name1[0]
            #print(File_name1)
            path=name.path
            Folder_Path= path.split("/",2)
            Folder_Path=Folder_Path[2]
            #print(Folder_Path)
            File_Type=File_name.split(".")
            File_Type=File_Type[1]
            #print(File_Type)
            Folder_Name= path.split("/",3)
            Folder_Name=Folder_Name[3]
            Folder_Name=Folder_Name.split("/")
            Folder_Name1=Folder_Name[0]
            #print(Folder_Name1)
            b = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
            today= b.strftime('%Y%m%d_%H%M%S')
            time=b.strftime('T%H%M%S')
            #print(today)
            #print(time)
            def f_get_delimiter(source_file_path):
                try:
                    headerlist=sc.textFile(source_file_path).take(1)
                    header_str=''.join(headerlist)
                    result=re.search("(,|;|\\\)",header_str)
                    return result.group()
                except Exception as err:
                    print("Error Occured", str (err))

            delimiter=(f_get_delimiter("dbfs:/mnt/source/Vehicle_Data/"+File_name1+".csv"))
            #print(delimiter)
            #type(delimiter)
            columns=CSV.columns
            #print(columns)
            #type(columns)
            FileColumns = '|'.join([str(elem) for elem in columns])
            #print(FileColumns)
            #type(FileColumns)
            b = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
            jobend= b.strftime('%Y%m%d_%H%M%S')
            jobendtime=b.strftime('T%H%M%S')
            CSV = CSV.select(lit( File_name1).alias("FileName"),lit(Folder_Name1).alias("FolderName"),lit(Folder_Path).alias("FilePath"),lit(File_Type).alias("FileType"),lit(FileColumns).alias("FileColumns"),lit(delimiter).alias("FileDelimiter"),lit(today).alias("JobStartDateTime"),lit(jobend).alias("JobEndDateTime"),lit(target).alias("TargetTable"))
            cond1= CSV.FileName.contains('Car details')
            cond2= CSV.FileName.contains('cost')
            CSV=CSV.withColumn("TargetTable", when(cond1,"Car_details.csv").when(cond2,"Car_Costing.csv").otherwise("Stg_"+File_name1+".csv"))
            CSV = CSV.distinct()
            display(CSV)
            df=df.union(CSV)
    #df.display()
    df=df.dropDuplicates(["FileName"])
    df.display()




# COMMAND ----------

from re import match
import re
from pyspark.sql.functions import col,lit,when
from datetime import datetime
import pytz
import pandas as pd
matched_files=[]
files=dbutils.fs.ls('/mnt/destination/')
source_files=dbutils.fs.ls('/mnt/source/Vehicle_Data/')
for file in files:
    #print(file)
    if(match(".*csv", file.name)): 
        matched_files.append(file.path)
        print("Metadata File  found, updating  metadata file")
metadata_file=matched_files
#print(metadata_file)
if not metadata_file:
    print("Metadata is not avaliable, creating first metadata file")
    for name in source_files:
        if(match(".*txt", name.name)):
            print("txt files are there in Source")
            TXT= spark.read.format("csv").option("header",True).load('/mnt/source/Vehicle_Data/'+name.name)
            display(TXT)
            File_name=name.name
            File_name1=File_name.split(".")
            File_name1=File_name1[0]
            print(File_name1)
            path=name.path
            Folder_Path= path.split("/",2)
            Folder_Path=Folder_Path[2]
            #print(Folder_Path)
            File_Type=File_name.split(".")
            File_Type=File_Type[1]
            #print(File_Type)
            Folder_Name= path.split("/",3)
            Folder_Name=Folder_Name[3]
            Folder_Name=Folder_Name.split("/")
            Folder_Name1=Folder_Name[0]
            #print(Folder_Name1)
            b = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
            today= b.strftime('%Y%m%d_%H%M%S')
            time=b.strftime('T%H%M%S')
            #print(today)
            #print(time)
            def f_get_delimiter(source_file_path):
                try:
                    headerlist=sc.textFile(source_file_path).take(1)
                    header_str=''.join(headerlist)
                    result=re.search("(,|;|\\\)",header_str)
                    return result.group()
                except Exception as err:
                    print("Error Occured", str (err))

            delimiter=(f_get_delimiter("dbfs:/mnt/source/Vehicle_Data/"+File_name1+".txt"))
            #print(delimiter)
            #type(delimiter)
            columns=TXT.columns
            #print(columns)
            #type(columns)
            FileColumns = '|'.join([str(elem) for elem in columns])
            #print(FileColumns)
            #type(FileColumns)
            b = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
            jobend= b.strftime('%Y%m%d_%H%M%S')
            jobendtime=b.strftime('T%H%M%S')
            TXT = TXT.select(lit(File_name1).alias("FileName"),lit(Folder_Name1).alias("FolderName"),lit(Folder_Path).alias("FilePath"),lit(File_Type).alias("FileType"),lit(FileColumns).alias("FileColumns"),lit(delimiter).alias("FileDelimiter"),lit(today).alias("JobStartDateTime"),lit(jobend).alias("JobEndDateTime"),lit(target).alias("TargetTable"))
            cond1= TXT.FileName.contains('details')
            cond2= TXT.FileName.contains('cost')
            TXT=TXT.withColumn("TargetTable", when (cond1,"Car_details.csv").when(cond2,"Car_Costing.csv").otherwise("Stg_"+File_name1+".csv"))
            TXT = TXT.distinct()
            display(TXT)
            df=df.union(TXT)
    df.display()
    print("Drop NA")       
    df=df.dropna()
    df=df.dropDuplicates(["FileName"])
    print("After union of csv metadata and source text file")
    df.display()
    



# COMMAND ----------

from re import match
import re
from pyspark.sql.functions import col,lit,when
from datetime import datetime
import pytz
import pandas as pd
matched_files=[]
files=dbutils.fs.ls('/mnt/destination/')
source_files=dbutils.fs.ls('/mnt/source/Vehicle_Data/')
for file in files:
    #print(file)
    if(match(".*csv", file.name)): 
        matched_files.append(file.path)
        print("Metadata File  found, updating  metadata file")
metadata_file=matched_files
#print(metadata_file)
if not metadata_file:
    print("Metadata is not avaliable, creating first metadata file")
    for name in source_files:
        if(match(".*xlsx", name.name)):
            print("xlsx files are there")
            XLSX= spark.read.format("com.crealytics.spark.excel").option("header",True).load("/mnt/source/Vehicle_Data/"+name.name)
            #display(XLSX)
            File_name=name.name
            print(File_name)
            File_name1=File_name.split(".")
            File_name1=File_name1[0]
            path=name.path
            Folder_Path= path.split("/",2)
            Folder_Path=Folder_Path[2]
            #print(Folder_Path)
            File_Type=File_name.split(".")
            File_Type=File_Type[1]
            #print(File_Type)
            Folder_Name= path.split("/",3)
            Folder_Name=Folder_Name[3]
            Folder_Name=Folder_Name.split("/")
            Folder_Name1=Folder_Name[0]
            #print(Folder_Name1)
            b = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
            today= b.strftime('%Y%m%d_%H%M%S')
            time=b.strftime('T%H%M%S')
            #print(today)
            #print(time)
            delimiter=(";")
            #print(delimiter)
            #type(delimiter)
            columns=XLSX.columns
            #print(columns)
            #type(columns)
            FileColumns = '|'.join([str(elem) for elem in columns])
            #print(FileColumns)
            #type(FileColumns)
            b = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
            jobend= b.strftime('%Y%m%d_%H%M%S')
            jobendtime=b.strftime('T%H%M%S')
            XLSX = XLSX.select(lit(File_name1).alias("FileName"),lit(Folder_Name1).alias("FolderName"),lit(Folder_Path).alias("FilePath"),lit(File_Type).alias("FileType"),lit(FileColumns).alias("FileColumns"),lit(delimiter).alias("FileDelimiter"),lit(today).alias("JobStartDateTime"),lit(jobend).alias("JobEndDateTime"),lit(target).alias("TargetTable"))
            #display(XLSX)
            cond1= XLSX.FileName.contains('details')
            cond2= XLSX.FileName.contains('cost')
            XLSX=XLSX.withColumn("TargetTable", when (cond1,"Car_details.csv").when(cond2,"Car_Costing.csv").otherwise("Stg_"+File_name1+".csv"))
            XLSX = XLSX.distinct()
            display(XLSX)
            df=df.union(XLSX)
    df.display()
    print("---Drop na---")
    df=df.dropna()
    df=df.dropDuplicates(["FileName"])
    print("----After union of csv, txt and excel----")
    df.display()
    pd = df.toPandas()
    pd.to_csv('/dbfs/mnt/destination/metadata.csv',sep=',',index=False,header=True)


# COMMAND ----------

from re import match
import pyspark.sql.functions as F
matched_files=[]
files=dbutils.fs.ls('/mnt/source/Vehicle_Data/')
for file in files:
    #print(file)
    if(match(".*csv", file.name)): 
        matched_files.append(file.path)
#vars=("Matched files: ",matched_files)
metadata_file1=matched_files
#print(metadata_file1)
if not metadata_file1:
   print('File Not found')
   
    

for name in files:
    #print(file)
    if(match(".*csv", name.name)):
        destination_file= spark.read.format("csv").option("header",True).load("dbfs:/mnt/destination/metadata.csv")
        display(destination_file) 
        CSV= spark.read.format("csv").option("header",True).load('/mnt/source/Vehicle_Data/'+name.name)
        display(CSV)
        File_name=name.name
        File_name1=File_name.split(".")
        File_name1=File_name1[0]
        print(File_name1)
        df = CSV.withColumn("File_Name", F.lit(File_name1))
        display(df)
        check=df.join(destination_file,(df.File_Name == destination_file.FileName),"left")
        check=check.drop("FileName","FolderName","FilePath","FileType","FileColumns","FileDelimiter","JobStartDateTime","JobEndDateTime")
        display(check)
        targetTable_value=check.first()['TargetTable']
        print(targetTable_value)
        target_path="/dbfs/mnt/destination/"+targetTable_value
        print(target_path)
        display(check)
        df_target=check
        df_target=df_target.drop("File_Name","TargetTable")
        pd = df_target.toPandas()
        pd.to_csv(target_path,sep=',',index=False,header=True)

    if(match(".*txt", name.name)):
        destination_file= spark.read.format("csv").option("header",True).load("dbfs:/mnt/destination/metadata.csv")
        display(destination_file)
        
        TXT= spark.read.format("csv").option("header",True).load('/mnt/source/Vehicle_Data/'+name.name)
        File_name=name.name
        File_name1=File_name.split(".")
        File_name1=File_name1[0]
        print(File_name1)
        df = TXT.withColumn("File_Name", F.lit(File_name1))
        display(df)
        check=df.join(destination_file,(df.File_Name == destination_file.FileName),"left")
        check=check.drop("FileName","FolderName","FilePath","FileType","FileColumns","FileDelimiter","JobStartDateTime","JobEndDateTime")
        display(check)
        targetTable_value=check.first()['TargetTable']
        print(targetTable_value)
        target_path="/dbfs/mnt/destination/"+targetTable_value
        print(target_path)
        display(check)
        df_target=check
        df_target=df_target.drop("File_Name","TargetTable")
        display(df_target)
        
        pd = df_target.toPandas()
        pd.to_csv(target_path,sep=',',index=False,header=True)

    if(match(".*xlsx", name.name)):
        destination_file= spark.read.format("csv").option("header",True).load("dbfs:/mnt/destination/metadata.csv")
        display(destination_file)
        
        XLSX= spark.read.format("com.crealytics.spark.excel").option("header",True).load("/mnt/source/Vehicle_Data/"+name.name)
        File_name=name.name
        File_name1=File_name.split(".")
        File_name1=File_name1[0]
        print(File_name1)
        df = XLSX.withColumn("File_Name", F.lit(File_name1))
        display(df)
        check=df.join(destination_file,(df.File_Name == destination_file.FileName),"left")
        check=check.drop("FileName","FolderName","FilePath","FileType","FileColumns","FileDelimiter","JobStartDateTime","JobEndDateTime")
        display(check)
        targetTable_value=check.first()['TargetTable']
        print(targetTable_value)
        target_path="/dbfs/mnt/destination/"+targetTable_value
        print(target_path)
        display(check)
        df_target=check
        df_target=df_target.drop("File_Name","TargetTable")
        display(df_target)
        
        pd = df_target.toPandas()
        pd.to_csv(target_path,sep=',',index=False,header=True)




