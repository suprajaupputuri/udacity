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

# Script generated for node Customer Trusted
CustomerTrusted_node1745416386235 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745416386235")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745416450853 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745416450853")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct customer_trusted.*
from accelerometer_trusted
inner join customer_trusted
on accelerometer_trusted.user = customer_trusted.email;
'''
SQLQuery_node1745416498716 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1745416450853, "customer_trusted":CustomerTrusted_node1745416386235}, transformation_ctx = "SQLQuery_node1745416498716")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745416498716, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745416381124", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1745416665977 = glueContext.getSink(path="s3://stedi-supraja/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1745416665977")
CustomerCurated_node1745416665977.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1745416665977.setFormat("json")
CustomerCurated_node1745416665977.writeFrame(SQLQuery_node1745416498716)
job.commit()
