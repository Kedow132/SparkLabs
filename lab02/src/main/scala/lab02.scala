import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{udf, col}
import java.net.URLDecoder.decode
import java.nio.charset.StandardCharsets


object lab02 extends App {
  def sparkJob(): Unit = {
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName("laba2_dv")
      .getOrCreate
    val csvOptions = Map(
      "inferSchema" -> "true",
      "delimiter" -> "\t",
      "encoding" -> "UTF-8"
    )
    val df = spark
              .read
              .options(csvOptions)
              .csv("/labs/laba02/logs")
    val newDf = df
                .withColumnRenamed("_c0", "id")
                .withColumnRenamed("_c1", "time")
                .withColumnRenamed("_c2", "url")
    val autousers = spark
                      .read
                      .json("/labs/laba02/autousers.json")
    val df_autousers = autousers
                          .select(explode(autousers("autousers")).alias("UID"))

    val myDecode = udf { (url: String) => try {
      decode(url, StandardCharsets.UTF_8.name())
    } catch {
              case _: Throwable => "error"
      }
    }

    val CleanData =
      newDf
        .na.drop(Seq("url", "id"))
        .filter(col("url").startsWith("http"))
        .withColumn("url", myDecode(col("url")))
        .withColumn("url", regexp_replace(col("url"), "(^www.)|(^(http[s]?://([w]{3}(.?)\\.)?)|(/.*$))", ""))


    val autousersList = df_autousers
                          .select("UID")
                          .map(r => r.getString(0))
                          .collect
                          .toList

    val urlsAutousers = CleanData
                          .filter(col("id").isin(autousersList:_*))
                          .select('url)
                          .distinct
    val numberOfAutousers = CleanData
                          .filter(col("id").isin(autousersList:_*))
                          .count
                          .toFloat
    val DFLength = CleanData.count.toFloat
    val urlAutousersList = urlsAutousers
                          .select("url")
                          .map(r => r.getString(0))
                          .collect
                          .toList
    val tmp = CleanData
                .groupBy('url).agg(
                                  count("*").alias("by_all"),
                                  count(when(
                                     (col("url").isin(urlAutousersList:_*)
                                      && col("id").isin(autousersList:_*), 1)
                                                        .alias("by_autousers"))))
                .orderBy(col("by_autousers").desc)

    val res = tmp
              .select(col("url").alias("domain"),
              format_number( (  (col("by_autousers") * col("by_autousers") / (DFLength * DFLength))
                              / ( (col("by_all") / DFLength ) * ( numberOfAutousers / DFLength) ), 15)
                                          .alias("relevance").cast("Decimal(16, 15)")))
              .orderBy(col("relevance").desc, 'domain.asc)
              .limit(200)

    res.coalesce(1)
      .write
      .option("header","true")
      .option("sep","\t")
      .mode("overwrite")
      .csv("/user/danila.logunov/laba02")

    spark.stop()
  }
sparkJob()
}
