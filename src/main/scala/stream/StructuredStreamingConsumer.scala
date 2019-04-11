package stream

// http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams.html

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._

import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._
import com.mapr.db.spark.streaming.MapRDBSourceConfig

/**
 * Consumes messages from a topic in MapR Streams using the Kafka interface,
 * enriches the message with  the k-means model cluster id and publishs the result in json format
 * to another topic
 * Usage: SparkKafkaConsumerProducer  <model> <topicssubscribe> <topicspublish>
 *
 *   <model>  is the path to the saved model
 *   <topic> is a  topic to consume from
 *   <tableName> is a table to write to
 * Example:
 *    $  spark-submit --class com.sparkkafka.uber.SparkKafkaConsumerProducer --master local[2] \
 * mapr-sparkml-streaming-uber-1.0.jar /user/user01/data/savemodel  /user/user01/stream:ubers /user/user01/stream:uberp
 *
 *    for more information
 *    http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
 */

object StructuredStreamingConsumer extends Serializable {

  val schema = StructType(Array(
    StructField("asin", StringType, true),
    StructField("helpful", ArrayType(StringType), true),
    StructField("overall", DoubleType, true),
    StructField("reviewText", StringType, true),
    StructField("reviewTime", StringType, true),
    StructField("reviewerID", StringType, true),
    StructField("reviewerName", StringType, true),
    StructField("summary", StringType, true),
    StructField("unixReviewTime", LongType, true)
  ))

  def main(args: Array[String]): Unit = {

    // MapR Event Store for Kafka Topic to read from 
    var topic: String = "/user/mapr/stream:reviews"
    // MapR Database table to write to 
    var tableName: String = "/user/mapr/reviewtable"
    // Directory to read the saved ML model from 
    var modeldirectory = "/user/mapr/sentmodel/"

    if (args.length == 3) {
      topic = args(0)
      modeldirectory = args(1)
      tableName = args(2)

    } else {
      System.out.println("Using hard coded parameters unless you specify topic model directory and table. <topic model table>   ")
    }

    val spark: SparkSession = SparkSession.builder().appName("stream").master("local[*]").getOrCreate()

    import spark.implicits._
    val model = org.apache.spark.ml.PipelineModel.load(modeldirectory)

    // get vocabulary from the Count Vectorizer
    val vocabulary = model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary
    // get the logistic regression model 
    val lrModel = model.stages.last.asInstanceOf[LogisticRegressionModel]
    // Get array of coefficients
    val weights = lrModel.coefficients.toArray
    // get array of word, and corresponding coefficient Array[(String, Double)]
    val word_weight = vocabulary.zip(weights)

    word_weight.sortBy(-_._2).take(5).foreach {
      case (word, weight) =>
        println(s"feature: $word, importance: $weight")
    }
    word_weight.sortBy(_._2).take(5).foreach {
      case (word, weight) =>
        println(s"feature: $word, importance: $weight")
    }

    val df1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "maprdemo:9092").option("subscribe", topic).option("group.id", "testgroup").option("startingOffsets", "earliest").option("failOnDataLoss", false).option("maxOffsetsPerTrigger", 1000).load()
    println(df1.printSchema)

    println("Enrich Transformm Stream")

    // cast the df1 column value to string
    // use the from_json function to convert value JSON string to flight schema
    val df2 = df1.select($"value" cast "string" as "json").select(from_json($"json", schema) as "data").select("data.*")
    df2.printSchema
    val df3 = df2.filter("overall !=3") // add column label
    val df4 = df3.withColumn("reviewTS", concat($"summary", lit(" "), $"reviewText"))

    val bucketizer = new Bucketizer().setInputCol("overall").setOutputCol("label").setSplits(Array(Double.NegativeInfinity, 3.0, Double.PositiveInfinity))
    val df5 = bucketizer.transform(df4)

    // transform the DataFrame with the model pipeline, which will tranform the features according to the pipeline, 
    // estimate and then return the predictions in a column of a new DateFrame
    val predictions = model.transform(df5)

    println("write stream")
    predictions.printSchema

    // drop the columns that we do not want to store 
    val df6 = predictions.drop("cv", "probability", "features", "helpful", "reviewTokensUf", "reviewTS", "rawPrediction")
    val df7 = df6.withColumn("_id", concat($"asin", lit("_"), $"unixReviewTime"))

    df7.printSchema

    import com.mapr.db.spark.impl._
    import com.mapr.db.spark.streaming._
    import com.mapr.db.spark.sql._
    import com.mapr.db.spark.streaming.MapRDBSourceConfig
    val writedb = df7.writeStream
      .format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, tableName)
      .option(MapRDBSourceConfig.IdFieldPathOption, "id")
      .option(MapRDBSourceConfig.CreateTableOption, false)
      .option("checkpointLocation", "/tmp/s")
      .option(MapRDBSourceConfig.BulkModeOption, true)
      .option(MapRDBSourceConfig.SampleSizeOption, 1000)
      .start()

    writedb.awaitTermination(300000)

  }

}