#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType

def admissions_schema():
     return StructType(
                        [StructField("PatientID",StringType(),True),
                         StructField("AdmissionID",StringType(),True),
                         StructField("AdmissionStartDate",StringType(),True),
                         StructField("AdmissionEndDate",StringType(),True),
                         StructField("PrimaryDiagnosisCode",StringType(),True),
                         StructField("PrimaryDiagnosisDescription",StringType(),True),
                        ]
                    )
def main():
    """main
    """
    spark = SparkSession.builder.appName("ExtractEventsJob").getOrCreate()

    admissions_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092")\
        .option("subscribe","admissions")\
        .load()
    
    admissions_clean = admissions_raw\
        .select(admissions_raw.value.cast('string').alias('raw_event'),\
        admissions_raw.timestamp.cast('string'),\
        from_json(admissions_raw.value.cast('string'),\
        admissions_schema()).alias('json')).select('raw_event', 'timestamp', 'json.*')

    sink = admissions_clean \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_admissions") \
        .option("path", "/tmp/admissions_rcd") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    sink.awaitTermination()

if __name__ == "__main__":
    main()