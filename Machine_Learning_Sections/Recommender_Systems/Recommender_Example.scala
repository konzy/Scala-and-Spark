import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql._
val spark = SparkSession.builder().getOrCreate()

val ratings = spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/tables/hivl15uf1480488667173/movie_ratings.csv")

ratings.head()
ratings.printSchema()

val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")
val model = als.fit(training)

// Evaluate the model by computing the average error from real rating
val predictions = model.transform(test)

// import to use abs()
import org.apache.spark.sql.functions._

val error = abs(predictions.col("rating")) - abs(predictions.col("prediction"))
//error.withColumn("id", when($"id".isNull, 0).otherwise(1)).show
//// Drop NaNs
//df.filter(error.isNotNull)
//
//error.na().drop().describe().show()
