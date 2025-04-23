import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
CustomerTrusted_node1745431881793 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-supraja/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1745431881793")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1745431882707 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-supraja/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1745431882707")

# Script generated for node Join
Join_node1745415021765 = Join.apply(frame1=CustomerTrusted_node1745431881793, frame2=AccelerometerLanding_node1745431882707, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1745415021765")

# Script generated for node Drop Fields
DropFields_node1745415057314 = DropFields.apply(frame=Join_node1745415021765, paths=["email", "phone"], transformation_ctx="DropFields_node1745415057314")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1745415057314, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745414939393", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745415201644 = glueContext.getSink(path="s3://stedi-supraja/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745415201644")
AmazonS3_node1745415201644.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1745415201644.setFormat("json")
AmazonS3_node1745415201644.writeFrame(DropFields_node1745415057314)
job.commit()
