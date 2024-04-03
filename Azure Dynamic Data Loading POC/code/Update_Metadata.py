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

# COMMAND ----------

from re import match
import re
from pyspark.sql.functions import col,lit,when
from datetime import datetime
import pytz
import pandas as pd
import pyspark.sql.functions as F
matchedsource_files=[]
files=dbutils.fs.ls('/mnt/destination/')
source_files=dbutils.fs.ls('/mnt/source/Vehicle_Data/')

for file in files:
    if(match("metadata.*csv", file.name)):
        print("Metadata file found")
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
                CSV = CSV.select(lit(File_name1).alias("FileName"),lit(Folder_Name1).alias("FolderName"),lit(Folder_Path).alias("FilePath"),lit(File_Type).alias("FileType"),lit(FileColumns).alias("FileColumns"),lit(delimiter).alias("FileDelimiter"),lit(today).alias("JobStartDateTime"),lit(jobend).alias("JobEndDateTime"),lit(target).alias("TargetTable"))
                cond1= CSV.FileName.contains('Car details')
                cond2= CSV.FileName.contains('cost')
                CSV=CSV.withColumn("TargetTable", when(cond1,"Car_details.csv")
                               .when(cond2,"Car_Costing.csv")
                               .otherwise("Stg_"+File_name1+".csv"))
                CSV = CSV.distinct()
                #display(CSV)
                df=df.union(CSV)
        #df.display()
        df=df.dropDuplicates(["FileName"])
        df.display()
        print("Metadata File  found, updating  metadata file")
        destination_file= spark.read.format("csv").option("header",True).load("dbfs:/mnt/destination/metadata.csv")
        #display(destination_file)
        destination_file=destination_file.withColumnRenamed("FileName","FileName1").withColumnRenamed("FolderName","FolderName1").withColumnRenamed("FilePath","FilePath1").withColumnRenamed("FileType","FileType1").withColumnRenamed("FileColumns","FileColumns1").withColumnRenamed("FileDelimiter","FileDelimiter1").withColumnRenamed("JobStartDateTime","JobStartDateTime1").withColumnRenamed("JobEndDateTime","JobEndDateTime1").withColumnRenamed("TargetTable","TargetTable1")
        display(destination_file)

        new=df.join(destination_file,((destination_file.FolderName1 == df.FolderName) & (destination_file.FileColumns1 == df.FileColumns)&(destination_file.FileType1 == df.FileType)),"left").select(destination_file.FileName1,destination_file.FolderName1,destination_file.FilePath1,destination_file.FileType1,destination_file.FileColumns1,destination_file.FileDelimiter1,destination_file.JobStartDateTime1,destination_file.JobEndDateTime1,destination_file.TargetTable1,df.FileName,df.FolderName,df.FilePath,df.FileType,df.FileColumns,df.FileDelimiter,df.JobStartDateTime,df.JobEndDateTime,df.TargetTable)
        print("The data after joining!")
        display(new)
        new=new.withColumn("FileName1",when(((~(new.FileName1 ==new.FileName))| new.FileName1.isNull()),new.FileName).otherwise(new.FileName1)).withColumn("FolderName1",when(((~(new.FolderName1 ==new.FolderName))| new.FolderName1.isNull()),new.FolderName).otherwise(new.FolderName)).withColumn("FilePath1",when(((~(new.FilePath1 ==new.FilePath))| new.FilePath1.isNull()),new.FilePath).otherwise(new.FilePath1)).withColumn("FileType1",when(((~(new.FileType1 ==new.FileType))| new.FileType1.isNull()),new.FileType).otherwise(new.FileType1)).withColumn("FileColumns1",when(((~(new.FileColumns1 ==new.FileColumns))| new.FileColumns1.isNull()),new.FileColumns).otherwise(new.FileColumns1)).withColumn("FileDelimiter1",when(((~(new.FileDelimiter1 ==new.FileDelimiter))| new.FileDelimiter1.isNull()),new.FileDelimiter).otherwise(new.FileDelimiter1)).withColumn("JobStartDateTime1",when(((~(new.JobStartDateTime1 ==new.JobStartDateTime))| new.JobStartDateTime1.isNull()),new.JobStartDateTime).otherwise(new.JobStartDateTime1)).withColumn("JobEndDateTime1",when(((~(new.JobEndDateTime1 ==new.JobEndDateTime))| new.JobEndDateTime1.isNull()),new.JobEndDateTime).otherwise(new.JobEndDateTime1)).withColumn("TargetTable1",when(((~(new.TargetTable1 ==new.TargetTable))| new.TargetTable1.isNull()),new.TargetTable1).otherwise(new.TargetTable))
        cond1=  new.TargetTable1.isNull()
        new=new.withColumn("TargetTable1", when(cond1,new.TargetTable).otherwise(new.TargetTable1))
        display(new)
        new=new.drop("FileName","FolderName","FilePath","FileType","FileColumns","FileDelimiter","JobStartDateTime","JobEndDateTime","TargetTable")
        display(new)
        new = new.dropDuplicates(["FileName1","FileColumns1"])
        new=new.withColumnRenamed("FileName1","FileName").withColumnRenamed("FolderName1","FolderName").withColumnRenamed("FilePath1","FilePath").withColumnRenamed("FileType1","FileType").withColumnRenamed("FileColumns1","FileColumns").withColumnRenamed("FileDelimiter1","FileDelimiter").withColumnRenamed("JobStartDateTime1","JobStartDateTime").withColumnRenamed("JobEndDateTime1","JobEndDateTime").withColumnRenamed("TargetTable1","TargetTable")
        display(new)
        for file in files:
            #print(file)
            if(match(".*csv", file.name)):
                matchedsource_files.append(file.path)
                File_name1=file.name
                File_name1=File_name.split(".")
                File_name1=File_name1[0]
                print(File_name1)
                update_csv = new.withColumn("File_Name", F.lit(File_name1))
                display(update_csv)



















# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

    new=destination_file.join(df,((destination_file.FolderName1 == df.FolderName) & (destination_file.FileColumns1 == df.FileColumns)&(destination_file.FileType1 == df.FileType)),"full").select(destination_file.FileName1,destination_file.FolderName1,destination_file.FilePath1,destination_file.FileType1,destination_file.FileColumns1,destination_file.FileDelimiter1,destination_file.JobStartDateTime1,destination_file.JobEndDateTime1,destination_file.TargetTable1,df.FileName,df.FolderName,df.FilePath,df.FileType,df.FileColumns,df.FileDelimiter,df.JobStartDateTime,df.JobEndDateTime,df.TargetTable)
        print("The data after joining!")
        display(new)
            

        new=new.withColumn("FileName1",when(((~(new.FileName1 ==new.FileName))| new.FileName1.isNull()),new.FileName).otherwise(new.FileName1)).withColumn("FolderName1",when(((~(new.FolderName1 ==new.FolderName))| new.FolderName1.isNull()),new.FolderName).otherwise(new.FolderName)).withColumn("FilePath1",when(((~(new.FilePath1 ==new.FilePath))| new.FilePath1.isNull()),new.FilePath).otherwise(new.FilePath1)).withColumn("FileType1",when(((~(new.FileType1 ==new.FileType))| new.FileType1.isNull()),new.FileType).otherwise(new.FileType1)).withColumn("FileColumns1",when(((~(new.FileColumns1 ==new.FileColumns))| new.FileColumns1.isNull()),new.FileColumns).otherwise(new.FileColumns1)).withColumn("FileDelimiter1",when(((~(new.FileDelimiter1 ==new.FileDelimiter))| new.FileDelimiter1.isNull()),new.FileDelimiter).otherwise(new.FileDelimiter1)).withColumn("JobStartDateTime1",when(((~(new.JobStartDateTime1 ==new.JobStartDateTime))| new.JobStartDateTime1.isNull()),new.JobStartDateTime).otherwise(new.JobStartDateTime1)).withColumn("JobEndDateTime1",when(((~(new.JobEndDateTime1 ==new.JobEndDateTime))| new.JobEndDateTime1.isNull()),new.JobEndDateTime).otherwise(new.JobEndDateTime1)).withColumn("TargetTable1",when(((~(new.TargetTable1 ==new.TargetTable))| new.TargetTable1.isNull()),new.TargetTable).otherwise(new.TargetTable))
        cond1=  new.TargetTable1.isNull()
        new=new.withColumn("TargetTable1", when(cond1,new.TargetTable).otherwise(new.TargetTable1))
        
        
            
        new=new.drop("FileName","FolderName","FilePath","FileType","FileColumns","FileDelimiter","JobStartDateTime","JobEndDateTime","TargetTable")
        display(new)
        new = new.dropDuplicates(["FileName1","FileColumns1"])
        new=new.withColumnRenamed("FileName1","FileName").withColumnRenamed("FolderName1","FolderName").withColumnRenamed("FilePath1","FilePath").withColumnRenamed("FileType1","FileType").withColumnRenamed("FileColumns1","FileColumns").withColumnRenamed("FileDelimiter1","FileDelimiter").withColumnRenamed("JobStartDateTime1","JobStartDateTime").withColumnRenamed("JobEndDateTime1","JobEndDateTime").withColumnRenamed("TargetTable1","TargetTable")
        display(new)
#pd = new.toPandas()
#pd.to_csv('/dbfs/mnt/destination/metadata.csv',sep=',',index=False,header=True)

        for name in source_files:
            if(match(".*txt", name.name)):
                print("txt files are there")

