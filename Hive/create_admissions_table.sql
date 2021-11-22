create external table if not exists default.admissions (
raw_event string,
timestamp string,
patientid string,
AdmissionID string,
AdmissionStartDate string,
AdmissionEndDate string,
PrimaryDiagnosisCode string,
PrimaryDiagnosisDescription string
) stored as parquet location '/tmp/admissions_rcd'
tblproperties ("parquet.compress"="SNAPPY");