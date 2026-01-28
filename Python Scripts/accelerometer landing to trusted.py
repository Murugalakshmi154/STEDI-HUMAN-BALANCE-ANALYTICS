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

# Script generated for node customer_trusted
customer_trusted_node1769087798911 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-human-balance-sowmi/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1769087798911")

# Script generated for node accelerometer_landing
accelerometer_landing_node1769087799585 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-sowmi/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1769087799585")

# Script generated for node join
SqlQuery4353 = '''
select a.user, a.timeStamp, 
a.x, a.y, a.z from a join c on a.user = c.email
'''
join_node1769087802731 = sparkSqlQuery(glueContext, query = SqlQuery4353, mapping = {"a":accelerometer_landing_node1769087799585, "c":customer_trusted_node1769087798911}, transformation_ctx = "join_node1769087802731")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=join_node1769087802731, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769087483627", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1769087805732 = glueContext.getSink(path="s3://stedi-human-balance-sowmi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1769087805732")
accelerometer_trusted_node1769087805732.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1769087805732.setFormat("glueparquet", compression="snappy")
accelerometer_trusted_node1769087805732.writeFrame(join_node1769087802731)
job.commit()