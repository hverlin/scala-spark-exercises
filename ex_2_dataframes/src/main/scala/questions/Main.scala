package questions

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("main").setMaster("local[*]")
    .set("spark.dynamicAllocation.enabled","true")
    .set("spark.shuffle.service.enabled","true")
      val sc = new SparkContext(conf)

      val sqlContext = new HiveContext(sc)

      val filePath = getClass().getResource("/2008.csv").toString
      val airportsPath = getClass().getResource("/airports.csv").toString
      val carriersPath = getClass().getResource("/carriers.csv").toString

    val processor = new AirTrafficProcessor(sqlContext, filePath, airportsPath, carriersPath)
    val data = processor.loadDataAndRegister(filePath)

    //println(data.schema)
    //data.collect().foreach(println)
    // println("<<<security>>>")
    // processor.cancelledDueToSecurity().show()
    // println("<<<weather dealy>>>")
    // processor.longestWeatherDelay().show()
    // println("<<<didn't fly>>>")
    //processor.didNotFly().show()
    // println("<<<from vegas to jfk>>>")
    // processor.flightsFromVegasToJFK().show()
    // println("<<<time taxiing>>>")
    // processor.timeSpentTaxiing().show()
    // println("<<<median>>>")
    // processor.distanceMedian().show()
    // println("<<<percentile>>>")
    // processor.score95().show()
    // println("<<<cancelled flights>>>")
    // processor.cancelledFlights().show()
    //println("least squares: " + processor.leastSquares())
    

    sc.stop()
  }

}