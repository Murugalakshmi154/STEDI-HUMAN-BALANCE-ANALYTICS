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

# Script generated for node customer_landing
customer_landing_node1769087498292 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-sowmi/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1769087498292")

# Script generated for node privacy filter
SqlQuery4275 = '''
select * from c where 
shareWithResearchAsOfDate is NOT NULL;
'''
privacyfilter_node1769087501653 = sparkSqlQuery(glueContext, query = SqlQuery4275, mapping = {"c":customer_landing_node1769087498292}, transformation_ctx = "privacyfilter_node1769087501653")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=privacyfilter_node1769087501653, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769087483627", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1769087504710 = glueContext.getSink(path="s3://stedi-human-balance-sowmi/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1769087504710")
customer_trusted_node1769087504710.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
customer_trusted_node1769087504710.setFormat("glueparquet", compression="snappy")
customer_trusted_node1769087504710.writeFrame(privacyfilter_node1769087501653)
job.commit()