import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

object train {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("lab07")
      .getOrCreate()

    import spark.implicits._

    val readKafka = spark.conf.get("spark.test.inputKafka", "danila_logunov")
    val kafkaOptions: Map[String, String] = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> readKafka
    )

    val modelPath = spark.conf.get("spark.test.model", "/user/danila.logunov/ml")
    val model = PipelineModel.load(modelPath)

    val sdf: DataFrame = spark
      .readStream
      .format("kafka")
      .options(kafkaOptions)
      .load

    val formattedDf = sdf.select(col("value").cast("string"))

    val schema = StructType(Array(
      StructField("uid", StringType, true),
      StructField("visits", ArrayType(StructType(Array
      (StructField("url", StringType, true),
        StructField("timestamp", LongType, true)
      ))))))

    val df = formattedDf
      .withColumn("value", from_json(col("value"), schema))
      .select("value.*")
      .withColumn("tmp", explode(col("visits")))
      .select("uid", "tmp.url")
      .withColumn("url", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("url", regexp_replace($"url", "www.", ""))
      .withColumn("url", regexp_replace($"url", "[.]", "-"))
      .groupBy("uid")
      .agg(collect_list("url").alias("domains"))

    val writeKafka = spark.conf.get("spark.test.outputKafka", "danila_logunov_lab07_out")

    val kafkaWriteOptions: Map[String, String] = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "topic" -> writeKafka
    )

    model
      .transform(df)
      .select(to_json(struct(col("uid"), col("gender_age"))).alias("value"))
      .writeStream
      .format("kafka")
      .options(kafkaWriteOptions)
      .option("checkpointLocation", "/user/danila.logunov/chk/ml/")
      .outputMode("update")
      .start
  }
}
