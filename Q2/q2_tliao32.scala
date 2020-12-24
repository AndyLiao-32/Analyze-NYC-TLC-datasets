// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.

// ENTER THE CODE BELOW
val customSchema2 = StructType(Array(StructField("LocationID", StringType, true), StructField("Borough", StringType, true), StructField("Zone", StringType, true), StructField("service_zone", StringType, true)))

val df2 = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema2)
   .load("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val df_1a = df_filter.withColumn("DOLocationID",col("DOLocationID").cast("int"))
                  .groupBy("DOLocationID").count()
                  .withColumnRenamed("count", "number_of_dropoffs")
                  .withColumn("number_of_dropoffs", col("number_of_dropoffs").cast("int"))
                  .sort(col("number_of_dropoffs").desc)
df_1a.show(5)


// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
val df_1b = df_filter.withColumn("PULocationID",col("PULocationID").cast("int"))
                  .groupBy("PULocationID").count()
                  .withColumnRenamed("count", "number_of_pickups")
                  .withColumn("number_of_pickups", col("number_of_pickups").cast("int"))
                  .sort(col("number_of_pickups").desc)
df_1b.show(5)

// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW
val df_2a = df_1a.withColumnRenamed("DOLocationID", "LocationID")
val df_2b = df_1b.withColumnRenamed("PULocationID", "LocationID")

val df_join = df_2a.join(df_2b, "LocationID")
                .withColumn("number_activities", col("number_of_dropoffs")+col("number_of_pickups"))
                .withColumn("number_activities", col("number_activities").cast("int"))
                .drop($"number_of_pickups")
                .drop($"number_of_dropoffs")
                .sort(col("number_activities").desc)

df_join.show(3)

// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW
val df_3 = df2.join(df_join, "LocationID")
              .groupBy("Borough")
              .agg(sum("number_activities"))
              .withColumnRenamed("sum(number_activities)", "total_number_activities")
              .sort(col("total_number_activities").desc)

df_3.show()

// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW
val df4 = df_filter.withColumn("date", to_date(col("pickup_datetime"),"yyyy-MM-dd"))
                   .groupBy("date").count()
                   .withColumn("day_of_week", date_format(col("date"), "EEEE"))
                   .withColumnRenamed("count", "pickup_count")

val df4_countDay = df4.groupBy("day_of_week").count()
                      .withColumnRenamed("count", "day_count")

val df4_avg_count = df4.groupBy("day_of_week")
                       .agg(sum("pickup_count"))
                       .withColumnRenamed("sum(pickup_count)", "pickup_count")

val df4_join = df4_countDay.join(df4_avg_count, "day_of_week")
                           .withColumn("avg_count", col("pickup_count") / col("day_count"))
                           .withColumn("avg_count", col("avg_count").cast("float"))
                           .drop($"pickup_count")
                           .drop($"day_count")
                           .sort(col("avg_count").desc)
                   

df4_join.show(2)

// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW
val df2_for5 = df2.filter(col("Borough") === "Brooklyn")

val df5 = df_filter.withColumn("HH time", date_format(col("pickup_datetime"),"HH"))
                  .withColumnRenamed("PULocationID", "LocationID")

val df5_join = df5.join(df2_for5, "LocationID")

val df5_count = df5_join.groupBy("LocationID", "HH time").count()
                        .withColumnRenamed("count", "max_count")
                        .join(df2_for5, "LocationID")
                        .drop($"Borough")
                        .drop($"service_zone")
                        .withColumnRenamed("Zone", "zone")

val df5_pickup_count = df5_join.groupBy("LocationID", "HH time").count()
                          .groupBy("HH time")
                          .agg(max("count"))
                          .withColumnRenamed("max(count)", "max2_count")
                          .withColumnRenamed("HH time", "hour_of_day")

val df5_result = df5_pickup_count.join(df5_count, df5_pickup_count("max2_count") === df5_count("max_count") && df5_pickup_count("hour_of_day") === df5_count("HH time"))
                          .drop($"LocationID")
                          .drop($"HH time")
                          .drop($"max_count")
                          .withColumn("max_count", col("max2_count"))
                          .drop($"max2_count")
                          .sort(col("hour_of_day"))
                          .withColumn("hour_of_day", col("hour_of_day").cast("int"))
                          .withColumn("max_count", col("max_count").cast("int"))

// df5_count.show(24)
// df5_pickup_count.show(24)
df5_result.show(24)

// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW
val windowSpec  = Window.partitionBy("Month").orderBy("dayOfMonth")

val df2_for6 = df2.filter(col("Borough") === "Manhattan")

val df_filter_for6 = df_filter.withColumn("Month", date_format(col("pickup_datetime"),"MMM"))
                              .filter(col("Month") === "Jan")
                              .withColumnRenamed("PULocationID", "LocationID")

val df6_join = df_filter_for6.join(df2_for6, "LocationID")
                             .withColumn("dayOfMonth", date_format(col("pickup_datetime"),"d"))

val df6_result = df6_join.groupBy("dayOfMonth", "Month").count()
                        .withColumn("dayOfMonth", col("dayOfMonth").cast("int"))
                        .sort(col("dayOfMonth"))
                        .withColumn("lag", lag("count", 1).over(windowSpec))
                        .withColumn("percent_change", (col("count")-col("lag"))*100/col("lag"))
                        .withColumn("percent_change", col("percent_change").cast("float"))
                        .sort(col("percent_change").desc)
                        .drop($"Month")
                        .drop($"count")
                        .drop($"lag")
                        .withColumnRenamed("dayOfMonth", "day")
                        .withColumn("percent_change", round(col("percent_change"), 2))

df6_result.show(3)
// df2_for6.show(5)
// df_filter_for6.show()
// df6_join.show()
