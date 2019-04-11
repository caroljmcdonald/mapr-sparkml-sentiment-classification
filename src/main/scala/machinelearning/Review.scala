package machinelearning

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.functions.{ concat, lit }

object Review {

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("flightdelay").master("local[*]").getOrCreate()

    var file = "/user/mapr/data/revsporttrain.json"

    var modeldirectory: String = "/user/mapr/sentmodel/"

    if (args.length == 1) {
      file = args(0)

    } else {
      System.out.println("Using hard coded parameters unless you specify the data file and test file. <datafile testfile>   ")
    }
    import spark.implicits._
    // With the SparkSession read method, read data from a file into a Dataset
    val df0 = spark.read.format("json").option("inferSchema", "true").load(file)

    // add column combining summary and review text, drop some others 
    val df = df0.withColumn("reviewTS", concat($"summary", lit(" "), $"reviewText")).drop("helpful").drop("reviewerID").drop("reviewerName").drop("reviewTime")

    df.show(5)
    val df1 = df.filter("overall !=3")
    df1.show
    df1.describe("overall").show

    val bucketizer = new Bucketizer().setInputCol("overall").setOutputCol("label").setSplits(Array(Double.NegativeInfinity, 4.0, Double.PositiveInfinity))

    val df2 = bucketizer.transform(df1)
    df2.cache
    df2.groupBy("overall", "label").count.show

    val fractions = Map(1.0 -> .1, 0.0 -> 1.0)
    val df3 = df2.stat.sampleBy("label", fractions, 36L)
    df3.groupBy("label").count.show

    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.8, 0.2), splitSeed)

    trainingData.cache
    trainingData.groupBy("label").count.show
    val tokenizer = new RegexTokenizer().setInputCol("reviewTS").setOutputCol("reviewTokensUf").setPattern("\\s+|[,.()\"]")
    val remover = new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setInputCol("reviewTokensUf").setOutputCol("reviewTokens")

    val cv = new CountVectorizer().setInputCol("reviewTokens").setOutputCol("cv").setVocabSize(200000) // .setMinDF(4)

    val idf = new IDF().setInputCol("cv").setOutputCol("features")

    //  regularizer parameters to encourage simple models and avoid overfitting.
    val lpar = 0.02
    val apar = 0.3

    // The final element in our ml pipeline is an Logistic Regression estimator  
    val lr = new LogisticRegression().setMaxIter(100).setRegParam(lpar).setElasticNetParam(apar)

    // Below we chain the stringindexers, vector assembler and logistic regressor in a Pipeline.
    val steps = Array(tokenizer, remover, cv, idf, lr)
    val pipeline = new Pipeline().setStages(steps)

    val model = pipeline.fit(trainingData)

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

    //transform the test set with the model pipeline,
    //which will map the features according to the same recipe
    val predictions = model.transform(testData)

    val evaluator = new BinaryClassificationEvaluator()
    val areaUnderROC = evaluator.evaluate(predictions)
    println("areaUnderROC " + areaUnderROC)

    val lp = predictions.select("prediction", "label")
    val counttotal = predictions.count().toDouble
    val correct = lp.filter("label == prediction").count().toDouble
    val wrong = lp.filter("label != prediction").count().toDouble
    val ratioWrong = wrong / counttotal
    val ratioCorrect = correct / counttotal
    val truen = (lp.filter($"label" === 0.0).filter($"label" === $"prediction").count()) / counttotal
    val truep = (lp.filter($"label" === 1.0).filter($"label" === $"prediction").count()) / counttotal
    val falsen = (lp.filter($"label" === 0.0).filter(not($"label" === $"prediction")).count()) / counttotal
    val falsep = (lp.filter($"label" === 1.0).filter(not($"label" === $"prediction")).count()) / counttotal

    println("ratio correct", ratioCorrect)

    println("true positive", truep)

    println("false positive", falsep)

    println("true negative", truen)

    println("false negative", falsen)

    model.write.overwrite().save(modeldirectory)

  }
}

