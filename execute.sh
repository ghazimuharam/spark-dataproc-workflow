#!/bin/bash
TEMPLATE_NAME="flights_etl"
CLUSTER_NAME="spark-job-flights"
BUCKET_NAME="gs://flights_data_etl"
REGION="asia-southeast2"
ZONE="asia-southeast2-b"
DATASET="data_flights"

BASE_DATE=$1
dt=$(date -d "$BASE_DATE" '+%Y-%m-%d');

# Initiate Google DataProc
gcloud beta dataproc workflow-templates create $TEMPLATE_NAME --region=$REGION &&

# Set cluster for workflow templates
gcloud beta dataproc workflow-templates set-managed-cluster $TEMPLATE_NAME \
--region=$REGION \
--zone=$ZONE \
--cluster-name=$CLUSTER_NAME \
 --scopes=default \
 --master-machine-type n1-standard-2 \
 --master-boot-disk-size 30 \
  --num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 30 \
--image-version 1.3 &&

gcloud dataproc workflow-templates \
 add-job pyspark $BUCKET_NAME/main.py \
--step-id flight_delays_etl_job \
--workflow-template=$TEMPLATE_NAME \
--region=$REGION -- $dt &&

# Instantiate Workflow Template
gcloud beta dataproc workflow-templates instantiate $TEMPLATE_NAME --region=$REGION &&

# Load ETL result to Google Big Query
bq load --source_format=NEWLINE_DELIMITED_JSON \
 $DATASET.flights_data \
 gs://flights_data_etl/Sources/$dt.json &&

bq load --source_format=NEWLINE_DELIMITED_JSON \
 $DATASET.avg_delay_distance_cat \
 gs://flights_data_etl/flights_data_output/$BASE_DATE"_distance_category"/*.json &&

bq load --source_format=NEWLINE_DELIMITED_JSON \
 $DATASET.avg_delay_flight_num \
 gs://flights_data_etl/flights_data_output/$BASE_DATE"_flight_nums"/*.json &&

# Delete Workflow Template
gcloud beta dataproc workflow-templates delete -q $TEMPLATE_NAME --region=$REGION