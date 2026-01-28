import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_curated
customer_curated_node1769088622683 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-human-balance-sowmi/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1769088622683")

# Script generated for node step_trainer_landing
step_trainer_landing_node1769088623274 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-sowmi/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1769088623274")

# Script generated for node join
SqlQuery4348 = '''
select s.serialNumber, s.sensorReadingTime,
s.distanceFromObject from s join c 
on s.serialNumber = c.serialNumber
'''
join_node1769088626335 = sparkSqlQuery(glueContext, query = SqlQuery4348, mapping = {"s":step_trainer_landing_node1769088623274, "c":customer_curated_node1769088622683}, transformation_ctx = "join_node1769088626335")

# Script generated for node drop duplicates
SqlQuery4347 = '''
select serialNumber,sensorReadingTime,
distanceFromObject from j
'''
dropduplicates_node1769088626721 = sparkSqlQuery(glueContext, query = SqlQuery4347, mapping = {"j":join_node1769088626335}, transformation_ctx = "dropduplicates_node1769088626721")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=dropduplicates_node1769088626721, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769087483627", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1769088631032 = glueContext.getSink(path="s3://stedi-human-balance-sowmi/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1769088631032")
step_trainer_trusted_node1769088631032.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1769088631032.setFormat("glueparquet", compression="snappy")
step_trainer_trusted_node1769088631032.writeFrame(dropduplicates_node1769088626721)
job.commit()