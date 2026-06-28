-- ----------------------------------------------------------------------------
-- 01: warehouse databases (staging -> ods -> dm)
-- Adapted for source_setup: hdfs:// LOCATIONs rehosted to ${SOURCE_WAREHOUSE}.
-- ----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS staging
  COMMENT 'Sqoop + SFTP landing mirrors (epoch dates live here)'
  LOCATION '${SOURCE_WAREHOUSE}/staging';

CREATE DATABASE IF NOT EXISTS ods
  COMMENT 'Cleansed / conformed / merged (all TIMESTAMPs)'
  LOCATION '${SOURCE_WAREHOUSE}/ods';

CREATE DATABASE IF NOT EXISTS dm
  COMMENT 'Dimensional marts + all views'
  LOCATION '${SOURCE_WAREHOUSE}/dm';
