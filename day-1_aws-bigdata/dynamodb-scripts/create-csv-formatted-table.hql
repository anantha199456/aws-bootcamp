
CREATE EXTERNAL TABLE s3_table(col1 bigint, col2 string, col3 string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 's3://optum-emr-lesson-ms/output/formatted/';