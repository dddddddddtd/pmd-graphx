import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import scala.io.Source

case class Station(vertexId: VertexId, stationId: String, stationName: String, installDockcount: Int, currentDockcount: Int)
case class Trip(tripId: Long, bikeId: String, fromStationId: String, toStationId: String)

object GraphXExample extends App {

  def readFile(filename: String) = {
    val src = Source.fromFile(filename)
    val lines = src.getLines().toList.drop(1).map(_.split(","))
    src.close()
    lines
  }

  def getStationVertexId(stationId: String): VertexId = {
    try {
      stations(stationId).vertexId
    } catch {
      case _ => -1L
    }
  }

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val stations = readFile("station.csv").zipWithIndex.map { case (array, index) =>
    val station = Station(index.toLong, array(0), array(1), array(5).toInt, array(7).toInt)
    station.stationId -> station
  }.toMap
  val trips = readFile("trip.csv").map(array => Trip(array(0).toLong, array(3), array(7), array(8)))

  val conf = new SparkConf().setAppName("GraphX Example").setMaster("local")
  val sc = new SparkContext(conf)

  val stationNodes: RDD[(VertexId, Station)] =
    sc.parallelize(stations.map{case (stationId, station) => (station.vertexId, station)}.toSeq)

  val tripEdges: RDD[Edge[Trip]] =
    sc.parallelize(trips.map(trip => {
      Edge(getStationVertexId(trip.fromStationId), getStationVertexId(trip.toStationId), trip)
    }))

  val graph = Graph(stationNodes, tripEdges)

  val bikeRides = graph.edges.groupBy(_.attr.bikeId).map{ case (bikeId, trips) => bikeId -> trips.toList.length}
  bikeRides.zipWithIndex().take(5).foreach{ case ((bike, count), index) => println(s"$index $bike: $count")}

  val maxRides = bikeRides.reduce((acc, value) => {
    if (acc._2 < value._2) value else acc
  })

  val mostUsedBikeGraph = Graph(stationNodes, tripEdges.filter(_.attr.bikeId == maxRides._1))

  println(s"graph edges: ${graph.edges.count()}")
  println(s"mostUsedBikeGraph edges: ${mostUsedBikeGraph.edges.count()}")
  val commonStartStation = mostUsedBikeGraph.outDegrees.reduce((acc, value) => if (acc._2<value._2) value else acc)
  val commonStopStation = mostUsedBikeGraph.inDegrees.reduce((acc, value) => if (acc._2<value._2) value else acc)

  println(s"Station with most departures based on graph: ${mostUsedBikeGraph.vertices.lookup(commonStartStation._1).head}")
  println(s"Station with most arrivals based on graph: ${mostUsedBikeGraph.vertices.lookup(commonStartStation._1).head}")

  // tylko dla sprawdzenia
  val usedbiketrips = trips.filter(_.bikeId == maxRides._1)
  val maxfromstation = usedbiketrips.map(_.fromStationId -> 1).groupBy(_._1).map{case (station, trips) => station -> trips.foldLeft(0)((acc, value) => acc+value._2)}
  val maxtostation = usedbiketrips.map(_.toStationId -> 1).groupBy(_._1).map{case (station, trips) => station -> trips.foldLeft(0)((acc, value) => acc+value._2)}
  println("Stations with most departures")
  maxfromstation.toList.sortWith(_._2 > _._2).take(3).foreach(println)
  println("Stations with most arrivals")
  maxtostation.toList.sortWith(_._2 > _._2).take(3).foreach(println)

}