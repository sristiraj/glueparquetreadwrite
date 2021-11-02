import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [input_data],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)


# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path=output_data,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)

S3bucket_node3.setCatalogInfo(
    catalogDatabase=glue_db, catalogTableName=glue_table
)

S3bucket_node3.setFormat("glueparquet")

S3bucket_node3.writeFrame(S3bucket_node1)

job.commit()
