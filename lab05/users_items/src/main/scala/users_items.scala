import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object users_items {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab05")
      .getOrCreate()

    val inputDir = spark.conf.get("spark.users_items.input_dir", "/user/danila.logunov/visits")
    val outputDir = spark.conf.get("spark.users_items.out,put_dir", "/user/danila.logunov/users-items")
    val update = spark.conf.get("spark.users_items.update", "0")

    val dfBuy= spark
      .read
      .format("json")
      .load(s"$inputDir/buy/")

    val dfView = spark
      .read
      .format("json")
      .load("$inputDir/view/")

    val dff = dfBuy
      .union(dfView)
      .withColumn("item_id", lower(regexp_replace(col("item_id"), "-{1,5}|\\s{1,5}}", "_")))
      .withColumn("item_id", concat(col("event_type"), col("item_id")))

    val date = dff.select(col("date")).orderBy(col("date").desc).first().mkString

    val df = dff
      .drop("p_date", "date")
      .na.drop(Seq("uid"))

    val matrix = df
              .groupBy(col("uid"))
              .pivot("item_id")
              .agg(count("*"))
              .na.fill(0)
    matrix
      .write
      .format("parquet")
      .mode(if(update.toInt == 1) "append" else "overwrite")
      .save(s"$outputDir/$date")

    spark.stop
  }
}
