# Cloud datalake on GCP
###Porject Summary
To migrate data from PostgreSQL to BigQuery by using Airflow.

### Data Source
- PostgreSQL

### Architecture
[![](overall_architech)](https://github.com/p-ploi/bluePi_exam_GCP/blob/f4dd0bf5d55cc6ba0b46527d0d00351aaaf7df39/img/1overall_architech.png)
*The Architecture for this project*

Let start to ELT process.
We will extract data from PostgreSQL to csv files and upload to *Staging Zone* (GCS).
Then, load data into *Raw Zone* (BQ).
Finally, we will transform data from *Raw Zone* (BQ) to *Persist Zone* (BQ).

[![](user_permission)](https://github.com/p-ploi/bluePi_exam_GCP/blob/f4dd0bf5d55cc6ba0b46527d0d00351aaaf7df39/img/3user_permission.png)

And user will use Analysis query in only at *Persist Zone*.

### Orchestration
An Airflow instance is deployed on a Google Cloud Composer to orchestrate the execution of our pipeline.
[![](overall_flow)](https://github.com/p-ploi/bluePi_exam_GCP/blob/f4dd0bf5d55cc6ba0b46527d0d00351aaaf7df39/img/2overall_flow.png)


#### Pipeline
##### Normal Process
1. **Extract** from PostgreSQL to csv files and upload to *Staging Zone* in Google Cloud Storage
	This pipeline is schedulling every hour.
	In *Staging Zone* will stored files with partition by date of extract and file name will auto running by datetime
	
2. **Load and Transform** data from *Staging Zone* to *Raw Zone*  in BigQuery
	This pipeline is schedulling every hour.
	In *Raw Zone*, the stored data will look like in a file.
	And we will transform it to real datatype and standard format with BQL
	
I seperated the extract pipeline and load & transform because if any process fails it will not depend.
But you can combine them in 1 pipeline by using 2 sub-pipelines.



#####  House keeping Process
The house keeping process is day-to-day cleanup and waste disposal
removal of unused files.
You can set data retention policy And also to comply with PDPA.

