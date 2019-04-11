package sparkmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._
import org.apache.log4j.{ Level, Logger }
//import com.fasterxml.jackson.annotation.{ JsonIgnoreProperties, JsonProperty }

object QueryReview {

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("asin", StringType, true),
    StructField("overall", DoubleType, true),
    StructField("reviewText", StringType, true),
    StructField("reviewTokens", ArrayType(StringType)),
    StructField("reviewTime", StringType, true),
    StructField("reviewerID", StringType, true),
    StructField("reviewerName", StringType, true),
    StructField("summary", StringType, true),
    StructField("label", StringType, true),
    StructField("prediction", StringType, true),
    StructField("unixReviewTime", LongType, true)
  ))

  def main(args: Array[String]) {

    var tableName: String = "/user/mapr/reviewtable"

    if (args.length == 1) {
      tableName = args(0)
    } else {
      System.out.println("Using hard coded parameters unless you specify the tablename ")
    }
    val spark: SparkSession = SparkSession.builder().appName("review").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._
    // load payment dataset from MapR-DB 
    val df = spark.sparkSession.loadFromMapRDB(tableName, schema)

    println("Reviews from MapR-DB")
    df.show

    df.createOrReplaceTempView("reviews")
    println("what is the count of predicted delay/notdelay for this dstream dataset")
    
    df.groupBy("prediction").count().show()
    
    df.groupBy("overall","asin").count.orderBy(desc("count")).show(5)

    df.select("summary","reviewText","overall","label","prediction").filter("asin='B004TNWD40'").show(5)
    
    val lp = df.select("label", "prediction")
    val counttotal = df.count()

    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    val truep = lp.filter($"prediction" === 1.0)
      .filter($"label" === $"prediction").count() / counttotal.toDouble
    val truen = lp.filter($"prediction" === 0.0)
      .filter($"label" === $"prediction").count() / counttotal.toDouble
    val falsep = lp.filter($"prediction" === 1.0)
      .filter(not($"label" === $"prediction")).count() / counttotal.toDouble
    val falsen = lp.filter($"prediction" === 0.0)
      .filter(not($"label" === $"prediction")).count() / counttotal.toDouble

    println("counttotal ", counttotal)
    println("ratio correct ", ratioCorrect)
    println("ratio wrong ", ratioWrong)
    println("correct ", correct)
    println("true positive ", truep)
    println("true negative ", truen)
    println("false positive ", falsep)
    println("false negative ", falsen)
  }
}

