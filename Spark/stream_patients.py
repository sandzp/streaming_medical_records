#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType

@udf('boolean')
def is_Defined_PatientRace(event_as_json):
    """udf for filtering events
    """
    event = json.loads(event_as_json)
    if event['PatientRace'] != 'Unknown':
        return True
    return False

@udf('boolean')
def is_Defined_PatientLanguage(event_as_json):
    """udf for filtering events
    """
    event = json.loads(event_as_json)
    if event['PatientLanguage'] != 'Unknown':
        return True
    return False

def patient_schema():
     return StructType(
                        [StructField("PatientID",StringType(),True),
                         StructField("PatientGender",StringType(),True),
                         StructField("PatientDateOfBirth",StringType(),True),
                         StructField("PatientRace",StringType(),True),
                         StructField("PatientMaritalStatus",StringType(),True),
                         StructField("PatientLanguage",StringType(),True),                                                                                                  
                         StructField("PatientPopulationPercentageBelowPoverty",StringType(),True)
                        ]
                    )
def main():
    """main
    """
    spark = SparkSession.builder.appName("ExtractEventsJob").getOrCreate()

    patients_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092")\
        .option("subscribe","patient")\
        .load()
    
    patients_clean = patients_raw\
        .filter(is_Defined_PatientRace(patients_raw.value.cast('string'))) \
        .filter(is_Defined_PatientLanguage(patients_raw.value.cast('string'))) \
        .select(patients_raw.value.cast('string').alias('raw_event'),\
        patients_raw.timestamp.cast('string'),\
        from_json(patients_raw.value.cast('string'),\
        patient_schema()).alias('json')).select('raw_event', 'timestamp', 'json.*')

    sink = patients_clean \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_patients") \
        .option("path", "/tmp/patients_rcd") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    sink.awaitTermination()

if __name__ == "__main__":
    main()