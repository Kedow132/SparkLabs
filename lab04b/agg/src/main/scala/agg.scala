import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object agg {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab04b")
      .getOrCreate()

    val kafkaOptions: Map[String, String] = Map(
      "kafka.bootstrap.servers" -> "kafka:ip",
      "subscribe" -> "danila_logunov"
    )

    val schema: StructType = StructType(Seq(
      StructField("event_type", StringType, nullable = true), StructField("category", StringType, nullable = true),
      StructField("item_id", StringType, nullable = true), StructField("item_price", IntegerType, nullable = true),
      StructField("uid", StringType, nullable = true), StructField("timestamp", LongType, nullable = true)
    ))

    val sdf: DataFrame = spark
      .readStream
      .format("kafka")
      .options(kafkaOptions)
      .load

    val formattedDf: DataFrame = sdf
      .select(col("value").cast("string"))
      .withColumn("value", from_json(col("value"), schema))
      .select("value.*")
      .withColumn("timestamp", (col("timestamp")  / 1000).cast("timestamp"))
      .groupBy(window(col("timestamp"), "1 hours").alias("wnd"))
      .agg(
        sum(when(col("event_type") === "buy", col("item_price"))
                      .otherwise(0)).alias("revenue"),
        sum(when(col("uid").isNotNull, 1).otherwise(0)).alias("visitors"),
        count(when(col("event_type") === "buy", 1).otherwise(0)).alias("purchases")
      )
      .select(to_json(struct(
        col("wnd.start").cast("long").alias("start_ts"),
        col("wnd.end").cast("long").alias("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue").cast("float") / col("purchases").cast("float")).alias("aov")
      )).alias("value"))


    val writeDf = formattedDf
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .option("checkpointLocation", "/user/danila.logunov/tmp/chk/lab04b")
      .option("kafka.bootstrap.servers", "kafka:ip")
      .option("topic", "danila_logunov_lab04b_out")
      .option("maxOffsetsPerTrigger", 200)

    writeDf.start().awaitTermination()

  }
}
