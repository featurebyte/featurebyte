-- url: https://storage.googleapis.com/featurebyte-public-datasets/healthcare_20230509.tar.gz
-- description: Healthcare Dataset

DROP DATABASE IF EXISTS HEALTHCARE CASCADE;
CREATE DATABASE HEALTHCARE;

-- populate Allergy
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/Allergy.parquet'
);
CREATE TABLE HEALTHCARE.__ALLERGY USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.ALLERGY AS
SELECT
  `AllergyGuid`,
  `PatientGuid`,
  `StartDate`,
  `AllergyType`,
  `ReactionName`,
  `Severity`,
  `record_available_at`
FROM HEALTHCARE.__ALLERGY
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate Diagnosis
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/Diagnosis.parquet'
);
CREATE TABLE HEALTHCARE.__DIAGNOSIS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.DIAGNOSIS AS
SELECT
  `RowID`,
  `DiagnosisGuid`,
  `PatientGuid`,
  `ValidFrom`,
  `ValidTo`,
  `ICD9Code`,
  `DiagnosisDescription`,
  `Acute`,
  `record_available_at`,
  `closedAt`
FROM HEALTHCARE.__DIAGNOSIS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate LabObservation
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/LabObservation.parquet'
);
CREATE TABLE HEALTHCARE.__LABOBSERVATION USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.LABOBSERVATION AS
SELECT
  `LabObservationGuid`,
  `LabResultGuid`,
  `HL7Text`,
  `ObservationValue`,
  `Units`,
  `ReferenceRange`,
  `AbnormalFlags`,
  `IsAbnormalValue`,
  `record_available_at`
FROM HEALTHCARE.__LABOBSERVATION
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate LabResult
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/LabResult.parquet'
);
CREATE TABLE HEALTHCARE.__LABRESULT USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.LABRESULT AS
SELECT
  `LabResultGuid`,
  `PatientGuid`,
  `ReportDate`,
  `tz_offset`,
  `record_available_at`
FROM HEALTHCARE.__LABRESULT
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate Patient
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/Patient.parquet'
);
CREATE TABLE HEALTHCARE.__PATIENT USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.PATIENT AS
SELECT
  `RowID`,
  `PatientGuid`,
  `Gender`,
  `DateOfBirth`,
  `StateCode`,
  `ValidFrom`,
  `record_available_at`,
  LAG(`ValidFrom`) OVER (PARTITION BY `PatientGuid` ORDER BY `ValidFrom` DESC) IS NULL AS `CurrentRecord`
FROM HEALTHCARE.__PATIENT
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate PatientSmokingStatus
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/PatientSmokingStatus.parquet'
);
CREATE TABLE HEALTHCARE.__PATIENTSMOKINGSTATUS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.PATIENTSMOKINGSTATUS AS
SELECT
  `PatientSmokingStatusGuid`,
  `PatientGuid`,
  `Description`,
  `NISTcode`,
  `ValidFrom`,
  `record_available_at`,
  LAG(`ValidFrom`) OVER (PARTITION BY `PatientSmokingStatusGuid` ORDER BY `ValidFrom` DESC) IS NULL AS `CurrentRecord`
FROM HEALTHCARE.__PATIENTSMOKINGSTATUS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate Prescription
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/Prescription.parquet'
);
CREATE TABLE HEALTHCARE.__PRESCRIPTION USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.PRESCRIPTION AS
SELECT
  `PrescriptionGuid`,
  `PatientGuid`,
  `PrescriptionDate`,
  `tz_offset`,
  `Quantity`,
  `NumberOfRefills`,
  `RefillAsNeeded`,
  `GenericAllowed`,
  `NdcCode`,
  `MedicationName`,
  `MedicationStrength`,
  `Schedule`,
  `record_available_at`
FROM HEALTHCARE.__PRESCRIPTION
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate StateDetails
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/StateDetails.parquet'
);
CREATE TABLE HEALTHCARE.__STATEDETAILS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.STATEDETAILS AS
SELECT
  `StateGuid`,
  `StateCode`,
  `StateName`,
  `CentroidLatitude`,
  `CentroidLongitude`,
  `Area`,
  `CensusRegion`,
  `HospitalCount`,
  `HospitalBedCount`,
  `BelowPovertyLevel`,
  `Aged65Plus`,
  `TotalPopulation`,
  `ValidFrom`,
  `record_available_at`
FROM HEALTHCARE.__STATEDETAILS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate Visit
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/Visit.parquet'
);
CREATE TABLE HEALTHCARE.__VISIT USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW HEALTHCARE.VISIT AS
SELECT
  `VisitGuid`,
  `PatientGuid`,
  `VisitDate`,
  `tz_offset`,
  `Height`,
  `Weight`,
  `BMI`,
  `SystolicBP`,
  `DiastolicBP`,
  `RespiratoryRate`,
  `Temperature`,
  `PhysicianSpecialty`,
  `record_available_at`
FROM HEALTHCARE.__VISIT
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate Icd9Hierarchy
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/ICD9Hierarchy.parquet'
);
CREATE TABLE HEALTHCARE.ICD9HIERARCHY USING DELTA AS SELECT * FROM temp_table;

-- populate MedicalProduct
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/MedicalProduct.parquet'
);
CREATE TABLE HEALTHCARE.MEDICALPRODUCT USING DELTA AS SELECT * FROM temp_table;

-- populate SpecialtyGroup
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/SpecialtyGroup.parquet'
);
CREATE TABLE HEALTHCARE.SPECIALTYGROUP USING DELTA AS SELECT * FROM temp_table;
