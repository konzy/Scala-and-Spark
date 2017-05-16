import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by konzy on 5/13/2017.
  */
object BasicStuff extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Basic Stuff")
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.csv("USA_Housing.csv")
  df.show()

  df.printSchema()

  df.select("Avg Area Income").show()


}
