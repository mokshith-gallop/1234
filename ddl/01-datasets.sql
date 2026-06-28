-- BigQuery dataset creation for NBCS migration.
-- This file is NOT applied as a harness DDL step (CREATE SCHEMA is not
-- CREATE TABLE/VIEW).  It's provided for manual/Terraform provisioning.

CREATE SCHEMA IF NOT EXISTS staging
  OPTIONS(description='Sqoop + SFTP landing mirrors (epoch dates live here)');

CREATE SCHEMA IF NOT EXISTS ods
  OPTIONS(description='Cleansed / conformed / merged (all TIMESTAMPs)');

CREATE SCHEMA IF NOT EXISTS dm
  OPTIONS(description='Dimensional marts + all views');

CREATE SCHEMA IF NOT EXISTS _etl_control
  OPTIONS(description='Operational tables: watermarks, DQ results, job audit');
