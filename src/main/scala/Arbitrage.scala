import org.apache.spark.sql.SparkSession
import scalaj.http.{Http, HttpRequest}

object Arbitrage extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()
  val data = spark.sparkContext.parallelize(
    Seq("I like Spark", "Spark is awesome", "My first Spark job is working now and is counting down these words")
  )
  val filtered = data.filter(line => line.contains("awesome"))
  filtered
    .collect().foreach(print)
  println("home is")
  println(spark.version)
  val request: HttpRequest = Http("http://date.jsontest.com/")

  val responseOne = request.asString
  println(s"response is $responseOne")

  new Coinbase(spark).currencies().show
}
