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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1769088146898 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-human-balance-sowmi/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1769088146898")

# Script generated for node customer_trusted
customer_trusted_node1769088147318 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-human-balance-sowmi/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1769088147318")

# Script generated for node join
SqlQuery4544 = '''
select * from a join c on a.user = c.email
'''
join_node1769088150067 = sparkSqlQuery(glueContext, query = SqlQuery4544, mapping = {"c":customer_trusted_node1769088147318, "a":accelerometer_trusted_node1769088146898}, transformation_ctx = "join_node1769088150067")

# Script generated for node drop duplicates
SqlQuery4543 = '''
select distinct customerName, email, phone,
birthDay, serialNumber, registrationDate, 
lastUpdateDate, shareWithResearchAsOfDate, 
shareWithPublicAsOfDate,
shareWithFriendsAsOfDate from j
'''
dropduplicates_node1769088150381 = sparkSqlQuery(glueContext, query = SqlQuery4543, mapping = {"j":join_node1769088150067}, transformation_ctx = "dropduplicates_node1769088150381")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=dropduplicates_node1769088150381, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769087483627", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769088153326 = glueContext.getSink(path="s3://stedi-human-balance-sowmi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769088153326")
AmazonS3_node1769088153326.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
AmazonS3_node1769088153326.setFormat("glueparquet", compression="snappy")
AmazonS3_node1769088153326.writeFrame(dropduplicates_node1769088150381)
job.commit()