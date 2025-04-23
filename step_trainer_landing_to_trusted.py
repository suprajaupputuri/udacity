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

# Script generated for node Customer Curated
CustomerCurated_node1745432654268 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-supraja/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1745432654268")

# Script generated for node step_trainer_landing
step_trainer_landing_node1745432782380 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-supraja/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1745432782380")

# Script generated for node SQL Query
SqlQuery0 = '''
select step_trainer_landing.*
from step_trainer_landing
inner join customer_curated
on step_trainer_landing.serialNumber = customer_curated.serialNumber
'''
SQLQuery_node1745419383320 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":CustomerCurated_node1745432654268, "step_trainer_landing":step_trainer_landing_node1745432782380}, transformation_ctx = "SQLQuery_node1745419383320")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745419383320, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745419002129", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1745419512468 = glueContext.getSink(path="s3://stedi-supraja/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1745419512468")
step_trainer_trusted_node1745419512468.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1745419512468.setFormat("json")
step_trainer_trusted_node1745419512468.writeFrame(SQLQuery_node1745419383320)
job.commit()
