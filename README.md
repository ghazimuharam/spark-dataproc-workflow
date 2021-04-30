# Spark Dataproc Workflow

This repository will be use to transform huge amount of data using Spark combining with Google Cloud Platform.

## Installation

Use git to clone this repository

```bash
git clone https://github.com/ghazimuharam/spark-dataproc-workflow.git
```

## Prerequisite

Make sure you have python 3.7 installed on your machine

```bash
> python --version
Python 3.7.10
```

To run the script in this repository, you need to install Google Cloud CLI Tools, you can refer to Google [Cloud SDK Docs](https://cloud.google.com/sdk/docs/quickstart) for the installation

## Usage

Before running the bash script, you have to specify your Google Cloud Configuration at `pre_execute.sh`

```bash
PROJECT_ID="week3-blank-space"
BUCKET_NAME="gs://flights_data_etl"
REGION="asia-southeast2"
DATASET="data_flights"
```

and `execute.sh`

```bash
TEMPLATE_NAME="flights_etl"
CLUSTER_NAME="spark-job-flights"
BUCKET_NAME="gs://flights_data_etl"
REGION="asia-southeast2"
ZONE="asia-southeast2-b"
DATASET="data_flights"
```

Put all of your json flights_data in Sources directory.

### Main

Before running the `execute.sh`, you have to run `pre_execute.sh` to automatically create

- Google Storage Bucket (Based on Configuration)

- Copy of data from local client to Google Storage Bucket

- Big Query Datasets (Based on Configuration)

- 3 Big Query Schema (flights_data, avg_delay_distance_cat, avg_delay_flight_num)

After successfully running `pre_execute.sh`, you can run the actual data workflow job by using command below

```bash
sh execute.sh 2021-04-27
```

The output will be similiar with images below

![carbon](https://user-images.githubusercontent.com/22569688/116721377-6fac7c00-aa07-11eb-9c55-bbd047b0bfad.png)

## License

[MIT](https://choosealicense.com/licenses/mit/)