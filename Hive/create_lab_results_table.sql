create external table if not exists default.lab_results (
raw_event string,
timestamp string,
patientid string,
AdmissionID string,
LabName string,
LabValue string,
LabUnits string,
LabDateTime string
) stored as parquet location '/tmp/lab_results_rcd'
tblproperties ("parquet.compress"="SNAPPY");