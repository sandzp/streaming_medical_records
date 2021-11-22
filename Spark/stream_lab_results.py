#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType

def lab_results_schema():
     return StructType(
                        [StructField("PatientID",StringType(),True),
                         StructField("AdmissionID",StringType(),True),
                         StructField("LabName",StringType(),True),
                         StructField("LabValue",StringType(),True),
                         StructField("LabUnits",StringType(),True),
                         StructField("LabDateTime",StringType(),True),
                        ]
                    )
def main():
    """main
    """
    spark = SparkSession.builder.appName("ExtractEventsJob").getOrCreate()

    lab_results_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092")\
        .option("subscribe","lab_results")\
        .load()
    
    lab_results_clean = lab_results_raw\
        .select(lab_results_raw.value.cast('string').alias('raw_event'),\
        lab_results_raw.timestamp.cast('string'),\
        from_json(lab_results_raw.value.cast('string'),\
        lab_results_schema()).alias('json')).select('raw_event', 'timestamp', 'json.*')
    
    sink = lab_results_clean \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_lab_results") \
        .option("path", "/tmp/lab_results_rcd") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    sink.awaitTermination()

if __name__ == "__main__":
    main()