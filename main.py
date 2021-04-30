#!/usr/bin/env python3
import argparse
"""
Module Docstring
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_replace, lit
import datetime
import os


__author__ = "Muhammad Ghazi Muharam"
__version__ = "0.1.0"
__license__ = "MIT"

BUCKET_NAME = "gs://flights_data_etl"


def main(args):
    base_time = datetime.datetime.strptime(
        args, '%Y-%m-%d')

    replace_time = (base_time
                    - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    # spark = SparkSession.builder.appName("Week3BlankSpace").getOrCreate()
    sc = SparkContext()
    spark = SQLContext(sc)

    # flights_data = spark.read.json(
    #     BUCKET_NAME + '/Sources/' + base_time.strftime("%Y-%m-%d") + '.json')

    flights_data = spark.read.json(
        './Sources/' + base_time.strftime("%Y-%m-%d") + '.json')

    flights_data = flights_data.drop("flight_date")
    flights_data = flights_data.withColumn(
        "flight_date", lit(replace_time))

    flights_data.registerTempTable("flights_data")

    avg_delays_by_flight_nums = spark.sql(
        """
        select
            flight_date,
            flight_num,
            round(avg(arrival_delay),2) as avg_arrival_delay,
            round(avg(departure_delay),2) as avg_departure_delay
        from
            flights_data
        group by
            flight_date,
            flight_num
        """
    )

    flights_data = spark.sql(
        """
        select 
            *,
            case 
                when distance between 0 and 500 then 1 
                when distance between 501 and 1000 then 2
                when distance between 1001 and 2000 then 3
                when distance between 2001 and 3000 then 4 
                when distance between 3001 and 4000 then 5 
                when distance between 4001 and 5000 then 6 
            END distance_category 
        from 
            flights_data 
        """
    )

    flights_data.registerTempTable("flights_data")

    avg_delays_by_distance_category = spark.sql(
        """
        select
            flight_date,
            distance_category,
            round(avg(arrival_delay),2) as avg_arrival_delay,
            round(avg(departure_delay),2) as avg_departure_delay
        from
            flights_data
        group by
            flight_date,
            distance_category
        """
    )

    # output_flight_nums = BUCKET_NAME + "/flights_data_output/" + \
    #     base_time.strftime("%Y-%m-%d")+"_flight_nums"

    # output_distance_category = BUCKET_NAME + "/flights_data_output/" + \
    #     base_time.strftime("%Y-%m-%d")+"_distance_category"

    output_flight_nums = "./flights_data_output/" + \
        base_time.strftime("%Y-%m-%d")+"_flight_nums.avro"

    output_distance_category = "./flights_data_output/" + \
        base_time.strftime("%Y-%m-%d")+"_distance_category.avro"

    avg_delays_by_flight_nums.coalesce(
        1).write.format("avro").save(output_flight_nums)
    avg_delays_by_distance_category.coalesce(
        1).write.format("avro").save(output_distance_category)


if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Required positional argument
    parser.add_argument("date", help="Required positional argument")

    args = parser.parse_args()
    main(args.date)
