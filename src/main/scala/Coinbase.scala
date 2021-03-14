import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scalaj.http.Http
import org.apache.spark.sql.SparkSession // note: intellij renders this as unused but it's required for spark functions

case class Candle(time: Int, low: Int, high: Int, open: Int, close: Int, volume: Int)
case class CurrencyPair(base: String, quote: String)
case class Result(body: String, nextStart: LocalDateTime)

object Weekday extends Enumeration {
  val Minutes = Value(60)
  val FiveMinutes = Value(300)
  val FifteenMinutes = Value(900)
  val OneHour = Value(3600)
  val SixHours = Value(21600)
  val Day = Value(86400)
}

class Coinbase(spark: SparkSession) {

  import spark.implicits._

  def fetchCoinbase(pair: String, granularity: Weekday.Value, start: LocalDateTime): Either[String, Result] = {
    val numberOfMinsThatFitInGranularity = granularity match {
      case Weekday.FifteenMinutes => 15 * 300
    }
    val formatter = DateTimeFormatter.ISO_DATE_TIME
    val end = start.plusMinutes(numberOfMinsThatFitInGranularity)
    val formattedStart = start.format(formatter)
    val formattedEnd = end.format(formatter)
    println("formatted start" + formattedStart)
    println("formatted end" + formattedEnd)
    start.format(DateTimeFormatter.BASIC_ISO_DATE)
    val url = s"https://api.pro.coinbase.com/products/$pair/candles?granularity=${granularity.id}&start=$formattedStart&end=$formattedEnd"
    println(url)
    val result = Http(url).asString

    result.code match {
      case 200 => Right(Result(result.body, end.plusMinutes(15)))
      case _ => Left("somethng went wrong")
    }

  }


  def currencies() = {
    val url = s"https://api.pro.coinbase.com/products"
    val bodyJson = Http(url).asString.body
    val ds = Seq(bodyJson).toDS()
    spark.read.json(ds)
  }

  def extractDsFromOutputString(candlesArrayString: String) = {
    val csv = candlesArrayString.substring(2, candlesArrayString.length - 2).replace("],[", "\n")

    //x.select("value").show
    val full = Seq(csv).toDS

    spark.read.csv(full)
  }
}
