# bigdata-procamp-hw7
Bigdata procamp hw #7. Orchestration Lab: Apache Airflow

The script contains an Airflow DAG definition. The DAG uses the next Airflow variables:
- gce_region - a google cloud redion, e.g `us-central1`
- gce_zone - a google cloud redion, e.g `us-central1-c`
- gcp_project - the google cloud project ID, e.g. `some-project-00000000`
- script_path - path to pyspark script to execute in Airflow, e.g `some-backet/job.py`
- input_bucket_name - name of a bucket with flight dataset. Inside the path each data batch shoudl be in subfolder /yyyy/MM/dd/HH/, e.g `some-backet`
- dataset_name - path in the bucket with the data, e.g. `flights`
- out_bucket_name - bucket to store results, e.g. `some-another-backet/lab7`

And in this example the DAG will take pyspark script `gs://some-backet/job.py`, try to reed data from `gc://some-backet/flights//yyyy/MM/dd/HH/` and save results to `gs://some-another-backet/lab7`.
yyyy, MM, dd, HH are respectively year, month, day and hour of the time new data has arrived. 

The pyspark from the previous lab: https://github.com/klymya/bigdata-procamp-hw6/blob/main/lab1/lab1.py