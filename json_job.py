import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import UnnestFrame
from pprint import pprint
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_DATA_PATH","OUTPUT_DATA_PATH","GLUE_DB_NAME","GLUE_TABLE_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


input_data = args["INPUT_DATA_PATH"]
output_data = args["OUTPUT_DATA_PATH"]
glue_db = args["GLUE_DB_NAME"]
glue_table = args["GLUE_TABLE_NAME"]


df = spark.read.option("multiline","true").json(input_data)
S3bucket_node1 = DynamicFrame.fromDF(df, glueContext, "S3bucket_node1")


dfun = UnnestFrame.apply(frame = S3bucket_node1) 
odf = dfun.toDF()
new_col = []

for col in odf.columns:
    new_col.append(col.replace(".","_") )   

print(new_col)
odf = odf.toDF(*new_col)

odf.write.format("parquet").mode("overwrite").option("path",output_data).saveAsTable(glue_db+"."+glue_table)





job.commit()
