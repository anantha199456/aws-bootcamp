# Assingment - 1 
The Module performs the following Functions:

* Read dataset from Newyork Taxi Records.
  Link : https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
* Creat two input directories in s3 , ex :
  s3://&lt;bucket-name&gt;/nyc-taxi-records/input/yellow-taxi/year=2020/
  s3://&lt;bucket-name&gt;/nyc-taxi-records/input/green-taxi/year=2020/
* Objective - objective is to find the average distance and average cost per mile for
  both green and yellow taxis in 2020.
* Read the input data from the S3 input path, apply aggregations with PySpark code,
  and  write the summarized output to the S3 output path 
     s3://&lt;bucket-name&gt;/nyc-taxi-records/output/.

# Architecture 

![alt text](https://github.com/anantha199456/aws-bootcamp/blob/main/Assignment-1/architecture-new.JPG)

# Output result using Step Function.

![alt text](Assignment-1/stepfunctions_graph (1).png)
