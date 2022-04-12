import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


class data_mart extends App {
  def spark_job(): Unit = {
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName("laba3_logunov")
      .getOrCreate

    spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
    spark.conf.set("spark.sql.catalog.csdra", "com.datastax.spark.connector.datasource.CassandraCatalog")
    spark.conf.set("spark.cassandra.connection.port", "9042")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")
    val url = "jdbc:postgresql://10.0.0.31:5432/labdata"

    val cassOptions = Map("table" -> "clients", "keyspace" -> "labdata")
    val postgresOptions = Map("url" -> url, "user" -> "danila_logunov",
                              "password" -> "494EzDF2", "dbtable" -> "domain_cats",
                              "driver" -> "org.postgresql.Driver")
    val esOptions = Map("es.nodes" -> "10.0.0.31:9200",
                        "es.batch.write.refresh" -> "false",
                        "es.nodes.wan.only" -> "true")

    val cassDf = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(cassOptions)
      .load()

    val esDf = spark
      .read
      .format("es")
      .options(esOptions)
      .load("visits*")

    val jsonDf = spark
      .read
      .format("json")
      .load("hdfs:///labs/laba03/weblogs.json")
      .toDF

    val postgreDF = spark
      .read
      .format("jdbc")
      .options(postgresOptions)
      .load()

    val preparedCategoryEsDF = esDf
      .withColumn("category",
        lower(concat(lit("shop_"),
          regexp_replace(col("category"), "\\s{1,10}|-{1,10}", "_")
        )))
      .withColumnRenamed("category", "category_shop")
      .filter(col("event_type") === "view")
      .dropDuplicates(Seq("uid", "timestamp"))

    val preparedCategoriesPostgreDF = postgreDF
      .withColumn("category",
        lower(concat(lit("web_"),
          regexp_replace(col("category"), "\\s{1,10}|-{1,10}", "_")
        )))
      .withColumnRenamed("domain", "url")
      .withColumnRenamed("category", "category_web")

    val jsonDfWithCleanUrl =
      jsonDf
        .withColumn("tmp", explode(col("visits")))
        .select(col("uid"),col("tmp.*"))
        .withColumn("url",
          regexp_replace(
            col("url"), "(^www.)|(^((http[s]?://){1,5}([w]{3}(\\.?)\\.)?)|(/.*$))", ""))
        .dropDuplicates

    val cassDfClassificated = cassDf
      .withColumn("age",
        when(col("age") >= 18 && col("age") <= 24 , "18-24")
          .when(col("age") >= 24 && col("age") <= 34, "25-34")
          .when(col("age") >= 34 && col("age") <= 44, "35-44")
          .when(col("age") >= 45 && col("age") <= 54, "45-54")
          .otherwise(">=55")
      )
      .withColumnRenamed("age", "age_cat")

    val a = preparedCategoryEsDF
      .groupBy(col("uid"))
      .pivot("category_shop")
      .agg(count("*"))

    val b = jsonDfWithCleanUrl
      .join(preparedCategoriesPostgreDF, Seq("url"), "inner")
      .groupBy("uid")
      .pivot("category_web")
      .agg(count("*"))

    val result = cassDfClassificated
      .join(b, Seq("uid"), "left")
      .join(a, Seq("uid"), "left")

    val urlOutput =  "jdbc:postgresql://10.0.0.31:5432/danila_logunov"
    val postgresOptionsOutput = Map("url" -> urlOutput, "user" -> "danila_logunov",
      "password" -> "494EzDF2", "dbtable" -> "clients",
      "driver" -> "org.postgresql.Driver")
    result
      .write
      .format("jdbc")
      .options(postgresOptionsOutput)
      .mode("append")
      .save

    spark.stop()
  }
  spark_job()
}
