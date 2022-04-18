import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object features {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("lab06")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val users_items: DataFrame = spark
      .read
      .format("parquet")
      .load("/user/danila.logunov/users-items/20200429")

    val jsonDf: DataFrame = spark
          .read
          .format("json")
          .load("hdfs:///labs/laba03/weblogs.json")
          .withColumn("tmp", explode(col("visits")))
          .select("uid","tmp.*")
          .withColumn("timestamp", from_unixtime(col("timestamp") / 1000, "UTC"))
          .na.drop(Seq("uid"))
          .withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST"))))
          .withColumn("url", regexp_replace(col("url"), "www.", ""))
          .withColumn("url", regexp_replace(col("url"), "[.]", "-"))

    val top1000: Array[String] = jsonDf
      .groupBy("url")
      .agg(count("*").alias("popularity"))
      .orderBy(col("popularity").desc)
      .na.drop
      .limit(1000)
      .select("url")
      .orderBy(col("url").asc)
      .collect
      .map(x => x.mkString)

    val vectorizedJsonDf: DataFrame = jsonDf
      .filter(col("url").isin(top1000:_*))
      .groupBy("uid")
      .pivot("url")
      .agg(count("*").alias("visit"))
      .na.fill(0)
      .withColumn("domain_features", array(top1000
        .map(x => col(x)):_*))
      .select("uid", "domain_features")

    val daysJsonDf: DataFrame = jsonDf
      .withColumn("day", date_format(col("timestamp").cast("timestamp"), "EEE"))
      .withColumn("day", concat(lit("web_day_"), lower(col("day"))))
      .groupBy("uid")
      .pivot("day")
      .agg(count("*"))
      .na.fill(0)

    val hoursJsonDf: DataFrame = jsonDf
      .withColumn("hours", date_format(col("timestamp").cast("timestamp"), "H"))

    val pivotHoursJsonDf: DataFrame = hoursJsonDf
      .withColumn("hours", concat(lit("web_hour_"), col("hours")))
      .groupBy("uid")
      .pivot("hours")
      .agg(count("*"))
      .na.fill(0)

    val categorizedJsonDf: DataFrame = hoursJsonDf
      .withColumn("cat", when(
        (col("hours") >= 9) && (col("hours") < 18), "web_fraction_work_hours")
        .when((col("hours") >= 18) && (col("hours") <= 23), "web_fraction_evening_hours")
        .otherwise("to_drop"))

      .groupBy("uid")
      .pivot("cat")
      .agg(count("*"))
      .drop(col("to_drop"))
      .na.fill(0)

    val result: DataFrame = users_items
      .join(vectorizedJsonDf, Seq("uid"), "full")
      .join(daysJsonDf, Seq("uid"), "full")
      .join(pivotHoursJsonDf, Seq("uid"), "full")
      .join(categorizedJsonDf, Seq("uid"), "full")

    result
      .write
      .format("parquet")
      .save("/user/danila.logunov/features")

    spark.stop()
  }
}
