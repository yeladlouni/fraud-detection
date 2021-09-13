import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SparkSession, functions}

object FraudDetection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("FraudDetection")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val schema = StructType(Array(
      StructField("step", StringType),
      StructField("type", StringType),
      StructField("amount", FloatType),
      StructField("nameOrig", StringType),
      StructField("oldbalanceOrg", FloatType),
      StructField("newbalanceOrig", FloatType),
      StructField("nameDest", StringType),
      StructField("oldbalanceDest", FloatType),
      StructField("newbalanceDest", FloatType),
      StructField("isFraud", BooleanType),
      StructField("isFlaggedFraud", BooleanType)))

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "fraud")
      .load()
      .select(from_json(col("value").cast("string"), schema) as "data")
      .select("data.*")



    df
      .writeStream
      .format("console")
      .start()
      .awaitTermination()

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("amount", "oldBalance"))
      .setOutputCol("features")

    val estimator = new GBTClassifier()
      .setLabelCol("isFraud")
      .setFeaturesCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(vectorAssembler, estimator))

    val model = pipeline.fit(df)

    val predictions = model.transform(df)

    predictions.show(truncate = false)*/
  }
}