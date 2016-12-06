package questions

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.math._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph


/** GeoProcessor provides functionalites to 
  * process country/city/location data.
  * We are using data from http://download.geonames.org/export/dump/
  * which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
  *
  * @param sc       reference to SparkContext
  * @param filePath path to file that should be modified
  */
class GeoProcessor(sc: SparkContext, filePath: String) {

  //read the file and create an RDD
  //DO NOT EDIT
  val file = sc.textFile(filePath)

  /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
  def filterData(data: RDD[String]): RDD[Array[String]] = {
    /* hint: you can first split each line into an array.
    * Columns are separated by tab ('\t') character.
    * Finally you should take the appropriate fields.
    * Function zipWithIndex might be useful.
    */
    data.map(s => {
      val fields = s.split('\t')
      Array(fields(1), fields(8), fields(16))
    })
  }


  /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data        an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
  def filterElevation(countryCode: String, data: RDD[Array[String]]): RDD[Int] = {
    data
      .filter(country => country(1) == countryCode)
      .map(country => country(2).toInt)
  }


  /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data : RDD containing only elevation information
    * @return The average elevation
    */
  def elevationAverage(data: RDD[Int]): Double = {
    data.sum() / data.count()
  }

  /** mostCommonWords calculates what is the most common
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '. 
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of 
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
  def mostCommonWords(data: RDD[Array[String]]): RDD[(String, Int)] = {
    data
      .flatMap(s => s(0).split(" "))
      .map(s => (s, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
  }

  /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
  def mostCommonCountry(data: RDD[Array[String]], path: String): String = {
    val csv = sc.textFile(path)

    val countryCode =
      data
        .map(s => (s(1), 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .first()

    val countries = csv
      .map(line => line.split(",")
        .map(elem => elem.trim))

    val rep = countries.filter(c => c(1) == countryCode._1)
    if (rep.count() > 0) {
      rep.first()(0)
    } else {
      ""
    }
  }

  //
  /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    * if you want to use helper functions, use variables as
    * functions, e.g
    * val distance = (lat: Double) => {...}
    *
    * @param lat  latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
  def hotelsInArea(lat: Double, long: Double): Int = {
    val MAX_DIST = 10.0 //max distance in km

    val haversine = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      val R = 6371.0 //radius in km

      val dLat = (lat2 - lat1).toRadians
      val dLon = (lon2 - lon1).toRadians

      val a = pow(sin(dLat / 2.0), 2.0) + pow(sin(dLon / 2.0), 2.0) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2.0 * asin(sqrt(a))
      R * c
    }

    file
      .map(s => {
        val fields = s.split('\t')
        Array(fields(2), fields(4), fields(5))
      })
      .filter(s => s(0).toUpperCase.contains("HOTEL"))
      .map(s => haversine(lat, long, s(1).toDouble, s(2).toDouble))
      .filter(distance => distance <= MAX_DIST)
      .count()
      .toInt
  }

  //GraphX exercises

  /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    * || ||
    * || ||
    * \   /
    * \ /
    * +
    *
    * _ 3 _
    * /' '\
    * (1)  (1)
    * /      \
    * 1--(2)--->2
    * \       /
    * \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/1.6.1/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *             from the resources folder
    * @return graphx graph
    *
    */
  def loadSocial(path: String): Graph[Int, Int] = {
    val pattern = """\s*(\d+)\s*\|\s*(\d+)\s*""".r

    val rawEdges: RDD[Seq[Long]] = sc.textFile(path)
      .flatMap(pattern.unapplySeq(_))
      .map(e => e.map(id => id.toLong))

    val vertices: RDD[(Long, Int)] = rawEdges
      .flatMap[Long](l => l)
      .distinct()
      .map(id => (id, id.toInt))

    val edges: RDD[Edge[Int]] = sc.makeRDD(rawEdges
      .countByValue()
      .map { case (e, n) => Edge[Int](e.head, e(1), n.toInt) }
      .toSeq)

    Graph[Int, Int](vertices, edges)

  }

  /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
  def mostActiveUser(graph: Graph[Int, Int]): Int = {
    graph.outDegrees.reduce((a, b) => if (a._2 > b._2) a else b)._1.toInt
  }

  /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
  def pageRankHighest(graph: Graph[Int, Int]): Int = {
    val vertices = graph.pageRank(0.005).vertices
    val sorted = vertices.sortBy(_._2, ascending = false)
    sorted.first()._1.toInt
  }
}

/**
  *
  * Change the student id
  */
object GeoProcessor {
  val studentId = "584788"
}