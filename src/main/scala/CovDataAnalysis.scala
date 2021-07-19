
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lower, to_date, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.typesafe.config.ConfigRenderOptions
import java.util.Properties

case class CovData(
                    submission_date: Option[java.sql.Date],
                    state: Option[String],
                    total_cases: Option[Int],
                    new_case: Option[Int],
                    total_deaths: Option[Int],
                    new_death: Option[Int]) {
  def case_rate_classfier(num: Option[Int]): String = {
    num match {
      case Some(x) => {
        x match {
          case y if (y <= 20) => "LOW"
          case y if (y > 20 && y <= 50) => "MED"
          case y if (y > 50) => "HIGH"
        }
      }
      case None => "LOW"
    }
  }

  def death_rate_classfier(num: Option[Int]): String = {
    num match {
      case Some(x) => {
        x match {
          case y if (y <= 5) => "LOW"
          case y if (y > 5 && y <= 10) => "MED"
          case y if (y > 10) => "HIGH"
        }
      }
      case None => "LOW"
    }
  }


}


case class ExtCovData(
                       submission_date: Option[java.sql.Date],
                       state: Option[String],
                       total_cases: Option[Int],
                       new_case: Option[Int],
                       total_deaths: Option[Int],
                       new_death: Option[Int],
                       cov_case_rate: String,
                       cov_death_rate: String
                     )


object CovDataAnalysis extends App {
  Logger.getRootLogger.setLevel(Level.INFO)
  val logger = Logger.getLogger(getClass.getName)

  val conf: Config = ConfigFactory.load()
  val mysqlConfig = conf.getConfig("mysql")
  val colList = List("submission_date", "state", "total_cases", "new_case", "total_deaths", "new_death")

  def stripQuotes(_str: String): Option[String] = {
    _str match {
      case d if (_str == null || _str.trim.isEmpty) => None
      case _ => Some(_str.filter(_ != '"'))
    }
  }

  val stripQuotesUDF = udf(stripQuotes(_: String))
  val renderOpts = ConfigRenderOptions.defaults.setOriginComments(false).setComments(false).setJson(false)
  logger.info("SQL Config :" + mysqlConfig.root().render(renderOpts))

  val spark = SparkSession
    .builder()
    .appName("Cov_Data_Analysis")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.read.option("header", "true").csv("US_COVID_DATA_SHORT_W_HEADER.csv")

  // Some of the columns have quotes in it - we need to remove them
  val strippedQuotesDf = lines.select(lines.columns.map(c => stripQuotesUDF(col(c)).alias(c)): _*)

  strippedQuotesDf.show(5)
  strippedQuotesDf.printSchema()


  val renamedDfWithCast = strippedQuotesDf
    .withColumn("submission_date", to_date($"submission_date", "MM/dd/yyyy"))
    .withColumn("total_cases", col("total_cases").cast(IntegerType))
    .withColumn("new_case", col("new_case").cast(IntegerType))
    .withColumn("total_deaths", col("total_deaths").cast(IntegerType))
    .withColumn("new_death", col("new_death").cast(IntegerType))


  val baseCovidDS = renamedDfWithCast.as[CovData]

  val extendedCovDS = baseCovidDS.map(x => ExtCovData(x.submission_date,
    x.state, x.total_cases, x.new_case, x.total_deaths, x.new_death, x.case_rate_classfier(x.new_case), x.death_rate_classfier(x.new_death)
  ))

  // Print some sample data  with some filtering
  extendedCovDS.filter(_.cov_case_rate == "MED").show(5)

  // Write Extended Covid Dataset to MySQL
  val properties = new Properties()
  properties.put("user", mysqlConfig.getString("user"))
  properties.put("password", mysqlConfig.getString("password"))
  properties.put("serverTimezone", "UTC")
  extendedCovDS.write.mode(SaveMode.Overwrite).jdbc(mysqlConfig.getString("url"), mysqlConfig.getString("table"), properties)


  // Print some Debug messages
  logger.info("Line Count := " + lines.count())
  logger.info("Final Dataset Count := " + extendedCovDS.count())

  spark.close()

}
