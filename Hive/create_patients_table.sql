create external table if not exists default.patients (
raw_event string,
timestamp string,
patientid string,
patientgender string,
patientdateofbirth string,
patientrace string,
patientmaritalstatus string,
patientlanguage string,
patientpopulationpercentagebelowpoverty string
) stored as parquet location '/tmp/patients_rcd'
tblproperties ("parquet.compress"="SNAPPY");