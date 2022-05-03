import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.Pipeline

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("lab07")
      .getOrCreate()

    import spark.implicits._

    val inputPath = spark.conf.get("spark.train.path_read", "/labs/laba07/laba07.json")
    val train = spark
      .read
      .format("json")
      .load(inputPath)
      .withColumn("tmp", explode(col("visits")))
      .select("gender_age", "tmp.url", "uid")
      .na.drop(Seq("uid"))
      .withColumn("url", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("url", regexp_replace($"url", "www.", ""))
      .withColumn("url", regexp_replace($"url", "[.]", "-"))
      .groupBy("uid", "gender_age")
      .agg(collect_list("url").alias("domains"))
      .withColumnRenamed("gender_age", "ga")

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("ga")
      .setOutputCol("label")
      .fit(train)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val decoder = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("gender_age")
      .setLabels(indexer.labels)


    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, decoder))

    val model = pipeline.fit(train)

    val pathWrite = spark.conf.get("spark.train.path_write", "/user/danila.logunov/ml")

    model
      .write
      .overwrite()
      .save(pathWrite)
  }
}
