#Unnest json and create flattened data
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Relationalize
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME","INPUT_DATA_PATH","OUTPUT_DATA_PATH","GLUE_DB_NAME","GLUE_TABLE_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


input_data = args["INPUT_DATA_PATH"]
output_data = args["OUTPUT_DATA_PATH"]
glue_db = args["GLUE_DB_NAME"]
glue_table = args["GLUE_TABLE_NAME"]

# Begin variables to customize with your information
glue_source_database = glue_db
glue_source_table = glue_table
glue_temp_storage = input_data+"temp"
glue_relationalize_output_s3_path = output_data
dfc_root_table_name = "root" 

print(dfc_root_table_name)
#default value is "roottable"
# End variables to customize with your information

glueContext = GlueContext(spark.sparkContext)
# datasource0 = glueContext.create_dynamic_frame.from_catalog(database = glue_source_database, table_name = glue_source_table, transformation_ctx = "datasource0")
df = spark.read.option("multiline","true").json(input_data)

datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")

print("read")
dfc = Relationalize.apply(frame = datasource0, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "dfc")
print("relationalized")
print(dfc.keys())
for key in dfc.keys():
    modified_key = key.replace(".","_").replace("@","_")
    obj = dfc.select(key)
    odf = obj.toDF()
    new_col = []
    print(odf.columns)
    for col in odf.columns:
        new_col.append(col.replace(".","_").replace("@","_") )   

    odf = odf.toDF(*new_col)
    print(odf.columns)
    odf.write.format("orc").mode("overwrite").option("path",output_data+"_"+modified_key).saveAsTable(glue_db+"."+glue_table+"_"+modified_key)
# blogdataoutput = glueContext.write_dynamic_frame.from_options(frame = bdata, connection_type = "s3", connection_options = {"path": glue_relationalize_output_s3_path}, format = "orc", transformation_ctx = "blogdataoutput")

