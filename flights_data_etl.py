import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from datetime import date

sc = SparkContext.getOrCreate()
spark = SQLContext(sc)

file_date_name = "2019-05-11"
flights_data = spark.read.json("gs://ryan-etl/flights_data/{}.json".format(file_date_name))

flights_data.registerTempTable("flights_data")

spark.sql("SELECT max(distance) FROM flights_data").show()

qry = """
        SELECT
            flight_date,
            round(avg(arrival_delay), 2) as avg_arrival_delay,
            round(avg(departure_delay), 2) as avg_departure_delay,
            flight_num
        FROM
            flights_data
        GROUP BY
            flight_num,
            flight_date
      """

avg_delays_by_flight_nums = spark.sql(qry)

query = """
        SELECT
            *,
            case
                when distance between 0 and 500 then 1
                when distance between 501 and 1000 then 2
                when distance between 1001 and 2000 then 3
                when distance between 2001 and 3000 then 4
                when distance between 3001 and 4000 then 5
                when distance between 4001 and 5000 then 6
            END distance_category
        FROM
            flights_data
        """

flights_data = spark.sql(query)

flights_data.registerTempTable("flights_data")

qry = """
        SELECT
            flight_date,
            round(avg(arrival_delay), 2) as avg_arrival_delay,
            round(avg(departure_delay), 2) as avg_departure_delay,
            distance_category
        FROM
            flights_data
        GROUP BY
            distance_category,
            flight_date
      """

avg_delays_by_distance_category = spark.sql(qry)

output_path_flight_nums = "gs://flights_data_transformed/{}/flight_nums_output".format(file_date_name)
output_path_distance_category = "gs://flights_data_transformed/{}/distance_category_output".format(file_date_name)

avg_delays_by_flight_nums.coalesce(1).write.format("json").save(output_path_flight_nums)
avg_delays_by_distance_category.coalesce(1).write.format("json").save(output_path_distance_category)
