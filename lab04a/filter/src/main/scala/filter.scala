import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util._

object filter {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
                                  .builder()
                                  .appName("lab04a")
                                  .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val offsetRaw = spark.conf.get("spark.filter.offset", "earliest")
    val topic = spark.conf.get("spark.filter.topic_name", "lab04_input_data")
    val path = spark.conf.get("spark.filter.output_dir_prefix", "/user/danila.logunov/visits")

    val offset: String = Try(offsetRaw.toInt)
      match {
      case Success(v) => s"""{\"${topic}\":{\"0\":${v}}}"""
      case Failure(_) => offsetRaw
      }
    val kafkaOptions = Map("kafka.bootstrap.servers" -> "spark-master-1:6667",
                            "subscribe" -> topic,
                            "maxOffsetsPerTrigger" -> "30",
                            "startingOffsets" -> offset,
                            "minPartitions" -> "5")

    val df = spark.read.format("kafka").options(kafkaOptions).load

    val schema = StructType(Seq(
      StructField("event_type", StringType, true), StructField("category", StringType, true),
      StructField("item_id", StringType, true), StructField("item_price", IntegerType, true),
      StructField("uid", StringType, true), StructField("timestamp", LongType, true)
    ))

    val formattedDf = df
      .select(col("value").cast("string"))
      .withColumn("value", from_json(col("value"), schema))
      .select("value.*")
      .withColumn("date", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))
      .withColumn("p_date", col("date"))


    formattedDf.filter(col("event_type") === "view")
        .write
        .format("json")
        .mode("overwrite")
        .partitionBy("p_date")
        .save(path + "/buy")

    formattedDf.filter(col("event_type") === "buy")
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy("p_date")
      .save(path + "/buy")

    spark.stop()
  }
}
