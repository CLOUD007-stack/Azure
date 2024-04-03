# Databricks notebook source
dbutils.widgets.text("input", "","") 

servername = dbutils.widgets.get("input") 
#servername="test"

#display(dbutils.fs.ls("dbfs:/mnt/source/Vehicle_Data"))


# COMMAND ----------

# Read metadata file
from re import match
from pyspark.sql import SparkSession
from pyspark.sql.types import *
matched_files=[]

dest_folder="dbfs:/mnt/destination/"
files=dbutils.fs.ls('/mnt/destination/')
column_schema = StructType([StructField('FileName', StringType(), True),                    
                          StructField('FolderName', StringType(), True),
                          StructField('ServerName', StringType(), True),
                          StructField('FilePath', StringType(), True), 
                          StructField('FileType', StringType(), True), 
                          StructField('FileColumns',StringType(), True),
                          StructField('FileDelimiter',StringType(), True), 
                          StructField('JobStartDateTime', StringType(), True), 
                          StructField('JobEndDateTime', StringType(), True), 
                          StructField('TargetTable', StringType(), True)])

def createmetadata():
    spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
    emp_RDD = spark.sparkContext.emptyRDD()    
    column_schema = StructType([StructField('FileName', StringType(), True),                    
                          StructField('FolderName', StringType(), True),
                          StructField('ServerName', StringType(), True),
                          StructField('FilePath', StringType(), True), 
                          StructField('FileType', StringType(), True), 
                          StructField('FileColumns',StringType(), True),
                          StructField('FileDelimiter',StringType(), True), 
                          StructField('JobStartDateTime', StringType(), True), 
                          StructField('JobEndDateTime', StringType(), True), 
                          StructField('TargetTable', StringType(), True)])
    df=spark.createDataFrame(data = emp_RDD, schema = column_schema)
    return df


for file in files:
    if(match("metadata.csv", file.name)):
        print("metadata file exists")
        df_metadata=spark.read.format("csv").option("header",True).load("dbfs:/mnt/destination/metadata.csv")
        matched_files.append(file.path)

vars=matched_files
if not vars:
    print("metadata file doesnt exists")
    df_metadata=createmetadata()
    #df_metadata = spark.createDataFrame(data = emp_RDD, schema = columns)

df_metadata.display()


# COMMAND ----------

# creating functions for getting metadata for input files
from pyspark.sql.functions import col,lit,when, concat
import re
from datetime import datetime
import pytz
def f_get_delimiter(source_file_path):
                try:
                    headerlist=sc.textFile(source_file_path).take(1)
                    header_str=''.join(headerlist)
                    result=re.search("(,|;|\\\)",header_str)
                    return result.group()
                except Exception as err:
                    print("Error Occured", str (err))


def createTargetTable(df):
    print('Inside CreateTargetTable function')
    cond1= df.FileName.contains('Car details')
    cond2= df.FileName.contains('cost')
    df=df.withColumn("Stg",lit("Stg_"))
    df=df.withColumn("TargetTable", when(cond1,"Car_details.csv")
                   .when(cond2,"Car_Costing.csv")
                   .otherwise(concat(df.Stg,df.FileName)))
    #df=df.withColumn('TargetTable',when())
    df=df.drop("Stg")
    return df


def getfilemetadata(name, path,columns):
    print('Inside getfilemetadata function')
    df_file_metadata=createmetadata()
    #df_file_metadata = spark.createDataFrame(data = emp_RDD, schema = columns)
    Folder_Path= path.split("/",2)
    Folder_Path=Folder_Path[2]
    #print(Folder_Path)
    File_Type=name.split(".")
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
    delim=(f_get_delimiter("dbfs:/mnt/source/Vehicle_Data/"+name))
    #print(delim)
    newRow = spark.createDataFrame([(name,Folder_Name1,servername,Folder_Path,File_Type,columns,delim,today,today,None)],column_schema)
    df_file_metadata=df_file_metadata.union(newRow)
    print('end of getmetadata function')
    return df_file_metadata


#df1=getfilemetadata("Car data.csv","dbfs:/mnt/source/Vehicle_Data/Car data.csv","name|year|selling_price|km_driven|fuel|seller_type|transmission|owner|mileage|engine|max_power|torque|seats")
#df1.display()

# COMMAND ----------

def preparetarget(df_new_metadata1,df_source):
    print('Inside preparetarget function')
    check_val=df_new_metadata1.filter(col("TargetTable").isNull()).count()
    #print(check_val)
    #if there is any new entry which doesnt exists in metadata then it should be created as new row
    if check_val>0:
        df_new_metadata1=df_new_metadata1.drop('TargetTable')
        df_new_metadata1=df_new_metadata1.withColumnRenamed("Src_TargetTable","TargetTable")
        #df_new_metadata1.display()
        df_new_metadata1=createTargetTable(df_new_metadata1)
        df_metadata=df_metadata.union(df_new_metadata1)
    # if it is already existing then dont do anything
    else:
        df_new_metadata1=df_new_metadata1.drop("Src_TargetTable")
    #print('printing final metadata display')
    #df_metadata.display()
    for key, value in df_new_metadata1.select('TargetTable').first().asDict().items():
        globals()[key] = value
    path=dest_folder+TargetTable
    print(path)
    target_matched=[]
    for file in files:
        if(file.name==TargetTable):
            print('TargetTable file exists')
            df_target=spark.read.format("csv").option("header",True).load(path)
            df_target=df_target.union(df_source)
            df_target=df_target.dropDuplicates()
            target_matched.append(file.path)
    vars_target=target_matched
    if not vars_target:
        df_target=df_source
    return df_target


# COMMAND ----------

# Read input source files in loop
from  pyspark.sql.functions import input_file_name, lit, array

src_folder="dbfs:/mnt/source/Vehicle_Data"
src_files=dbutils.fs.ls('/mnt/source/Vehicle_Data/')

for src_file in src_files:
    #print('src file name is ',src_file.name)
    if(match(".*csv", src_file.name)):
        print('file name is ',src_file.name)
        print('file path is ',src_file.path)
        df_file_metadata=createmetadata()
        #df_file_metadata = spark.createDataFrame(data = emp_RDD, schema = columns)
        df_csv= spark.read.format("csv").option("header",True).load('/mnt/source/Vehicle_Data/'+src_file.name)
        columnlist=df_csv.columns
        newcolumn="|".join(columnlist)
        #print(newcolumn)
        df_file_metadata_dtls=getfilemetadata(src_file.name,src_file.path,newcolumn)
    
        # prepare metadata entry if required
        df_file_metadata_dtls=df_file_metadata_dtls.withColumnRenamed('TargetTable','Src_TargetTable')
        df_new_metadata=df_file_metadata_dtls.join(df_metadata,((df_metadata.FolderName==df_file_metadata_dtls.FolderName) & (df_metadata.FileType==df_file_metadata_dtls.FileType) & (df_metadata.FileColumns==df_file_metadata_dtls.FileColumns)),"left").select(df_file_metadata_dtls.FileName,df_file_metadata_dtls.FolderName,df_file_metadata_dtls.ServerName,df_file_metadata_dtls.FilePath,df_file_metadata_dtls.FileType,df_file_metadata_dtls.FileColumns,df_file_metadata_dtls.FileDelimiter,df_file_metadata_dtls.JobStartDateTime,df_file_metadata_dtls.JobEndDateTime, df_file_metadata_dtls.Src_TargetTable, df_metadata.TargetTable )
        #df_new_metadata.display()
        #Check if match is found from metadata table
        #df_target=preparetarget(df_new_metadata,df_csv)


        
        check_val=df_new_metadata.filter(col("TargetTable").isNull()).count()
        print(check_val)
        #if there is any new entry which doesnt exists in metadata then it should be created as new row
        if check_val>0:
            df_new_metadata=df_new_metadata.drop('TargetTable')
            df_new_metadata=df_new_metadata.withColumnRenamed("Src_TargetTable","TargetTable")
            #df_new_metadata.display()
            df_new_metadata=createTargetTable(df_new_metadata)
            df_metadata=df_metadata.union(df_new_metadata)
        # if it is already existing then dont do anything
        else:
            df_new_metadata=df_new_metadata.drop("Src_TargetTable")
        #print('printing final metadata display')
        #df_metadata.display()

        for key, value in df_new_metadata.select('TargetTable').first().asDict().items():
            globals()[key] = value
        path=dest_folder+TargetTable
        print(path)
        target_matched=[]

        for file in files:
            if(file.name==TargetTable):
                print('TargetTable file exists')
                df_target=spark.read.format("csv").option("header",True).load(path)
                df_target=df_target.union(df_csv)
                df_target=df_target.dropDuplicates()
                target_matched.append(file.path)
        vars_target=target_matched
        if not vars_target:
            df_target=df_csv
        

        #writing target to destination
        pd = df_target.toPandas()
        pandas_path='/dbfs/mnt/destination/'+TargetTable
        pd.to_csv(pandas_path,sep=',',index=False,header=True)

    if(match(".*txt", src_file.name)):
        df_txt= spark.read.format("csv").option("header",True).load('/mnt/source/Vehicle_Data/'+src_file.name)
        print('file name is ',src_file.name)
        print('file path is ',src_file.path)
        df_file_metadata=createmetadata()
        columnlist=df_txt.columns
        newcolumn="|".join(columnlist)
        #print(newcolumn)
        df_file_metadata_dtls=getfilemetadata(src_file.name,src_file.path,newcolumn)
    
        # prepare metadata entry if required
        df_file_metadata_dtls=df_file_metadata_dtls.withColumnRenamed('TargetTable','Src_TargetTable')
        df_new_metadata=df_file_metadata_dtls.join(df_metadata,((df_metadata.FolderName==df_file_metadata_dtls.FolderName) & (df_metadata.FileType==df_file_metadata_dtls.FileType) & (df_metadata.FileColumns==df_file_metadata_dtls.FileColumns)),"left").select(df_file_metadata_dtls.FileName,df_file_metadata_dtls.FolderName,df_file_metadata_dtls.ServerName,df_file_metadata_dtls.FilePath,df_file_metadata_dtls.FileType,df_file_metadata_dtls.FileColumns,df_file_metadata_dtls.FileDelimiter,df_file_metadata_dtls.JobStartDateTime,df_file_metadata_dtls.JobEndDateTime, df_file_metadata_dtls.Src_TargetTable, df_metadata.TargetTable )
        #df_new_metadata.display()
        #Check if match is found from metadata table
        #df_target=preparetarget(df_new_metadata,df_txt)


        check_val=df_new_metadata.filter(col("TargetTable").isNull()).count()
        print(check_val)
        #if there is any new entry which doesnt exists in metadata then it should be created as new row
        if check_val>0:
            df_new_metadata=df_new_metadata.drop('TargetTable')
            df_new_metadata=df_new_metadata.withColumnRenamed("Src_TargetTable","TargetTable")
            #df_new_metadata.display()
            df_new_metadata=createTargetTable(df_new_metadata)
            df_metadata=df_metadata.union(df_new_metadata)
        # if it is already existing then dont do anything
        else:
            df_new_metadata=df_new_metadata.drop("Src_TargetTable")
        #print('printing final metadata display')
        #df_metadata.display()

        for key, value in df_new_metadata.select('TargetTable').first().asDict().items():
            globals()[key] = value
        path=dest_folder+TargetTable
        print(path)
        target_matched=[]

        for file in files:
            if(file.name==TargetTable):
                print('TargetTable file exists')
                df_target=spark.read.format("csv").option("header",True).load(path)
                df_target=df_target.union(df_txt)
                df_target=df_target.dropDuplicates()
                target_matched.append(file.path)
        vars_target=target_matched
        if not vars_target:
            df_target=df_txt
        
        #writing target to destination
        pd = df_target.toPandas()
        new_targetfile=TargetTable.split('.')
        new_targetfile=new_targetfile[0]
        pandas_path='/dbfs/mnt/destination/'+new_targetfile+'.csv'
        pd.to_csv(pandas_path,sep=',',index=False,header=True)

    # Handling of XLSX files in the source
    if(match(".*xlsx", src_file.name)):
        df_xlsx = spark.read.format("com.crealytics.spark.excel").option("header",True).load("/mnt/source/Vehicle_Data/"+src_file.name)
        print('file name is ',src_file.name)
        print('file path is ',src_file.path)
        df_file_metadata=createmetadata()
        columnlist=df_xlsx.columns
        newcolumn="|".join(columnlist)
        #print(newcolumn)
        df_file_metadata_dtls=getfilemetadata(src_file.name,src_file.path,newcolumn)
        
    
        # prepare metadata entry if required
        df_file_metadata_dtls=df_file_metadata_dtls.withColumnRenamed('TargetTable','Src_TargetTable')
        df_new_metadata=df_file_metadata_dtls.join(df_metadata,((df_metadata.FolderName==df_file_metadata_dtls.FolderName) & (df_metadata.FileType==df_file_metadata_dtls.FileType) & (df_metadata.FileColumns==df_file_metadata_dtls.FileColumns)),"left").select(df_file_metadata_dtls.FileName,df_file_metadata_dtls.FolderName,df_file_metadata_dtls.ServerName,df_file_metadata_dtls.FilePath,df_file_metadata_dtls.FileType,df_file_metadata_dtls.FileColumns,df_file_metadata_dtls.FileDelimiter,df_file_metadata_dtls.JobStartDateTime,df_file_metadata_dtls.JobEndDateTime, df_file_metadata_dtls.Src_TargetTable, df_metadata.TargetTable )
        #df_new_metadata.display()
        #Check if match is found from metadata table
        #df_target=preparetarget(df_new_metadata,df_xlsx)


        check_val=df_new_metadata.filter(col("TargetTable").isNull()).count()
        print(check_val)
        #if there is any new entry which doesnt exists in metadata then it should be created as new row
        if check_val>0:
            df_new_metadata=df_new_metadata.drop('TargetTable')
            df_new_metadata=df_new_metadata.withColumnRenamed("Src_TargetTable","TargetTable")
            #df_new_metadata.display()
            df_new_metadata=createTargetTable(df_new_metadata)
            df_metadata=df_metadata.union(df_new_metadata)
        # if it is already existing then dont do anything
        else:
            df_new_metadata=df_new_metadata.drop("Src_TargetTable")
        #print('printing final metadata display')
        #df_metadata.display()

        for key, value in df_new_metadata.select('TargetTable').first().asDict().items():
            globals()[key] = value
        path=dest_folder+TargetTable
        print(path)
        target_matched=[]

        for file in files:
            if(file.name==TargetTable):
                print('TargetTable file exists')
                df_target=spark.read.format("csv").option("header",True).load(path)
                df_target=df_target.union(df_xlsx)
                df_target=df_target.dropDuplicates()
                target_matched.append(file.path)
        vars_target=target_matched
        if not vars_target:
            df_target=df_xlsx
    
        #writing target to destination
        pd = df_target.toPandas()
        new_targetfile=TargetTable.split('.')
        new_targetfile=new_targetfile[0]
        pandas_path='/dbfs/mnt/destination/'+new_targetfile+'.csv'
        pd.to_csv(pandas_path,sep=',',index=False,header=True)


#finally writing metadata to destination
pd1=df_metadata.toPandas()
pd1.to_csv('/dbfs/mnt/destination/metadata.csv',sep=',',index=False,header=True)
        




        
    




# COMMAND ----------


