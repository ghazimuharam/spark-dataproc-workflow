#!/bin/bash
PROJECT_ID="week3-blank-space"
BUCKET_NAME="gs://flights_data_etl"
REGION="asia-southeast2"
DATASET="data_flights"

# Create a Google Bucket
gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION -b on $BUCKET_NAME

DATE=2021-04-27

file_date=$(date -d "$DATE" +%Y-%m-%d)
iter=1

files=`ls ./Sources/*.json`
for file in $files
do
    mv $file ${file//$(basename $file .json)/${file_date}}
    gsutil cp $file $BUCKET_NAME"/Sources/"
    file_date=$(date -d "$DATE + $iter day" +%Y-%m-%d)
    iter=$((iter + 1))
done

gsutil cp ./main.py $BUCKET_NAME

# Create Big Query Datasets
bq --location=$REGION --project_id $PROJECT_ID mk -d $DATASET

# Create Big Query flights_data Schema
bq --project_id $PROJECT_ID mk -t --schema "./schema/flights_data.json" $DATASET.flights_data

# Create Big Query avg_delay_distance_cat Schema
bq --project_id $PROJECT_ID mk -t --schema "./schema/avg_delay_distance_cat.json" $DATASET.avg_delay_distance_cat

# Create Big Query avg_delay_flight_num Schema
bq --project_id $PROJECT_ID mk -t --schema "./schema/avg_delay_flight_num.json" $DATASET.avg_delay_flight_num