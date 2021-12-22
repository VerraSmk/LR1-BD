import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.lang.Math._
import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import java.util.Locale


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(cfg)

    val tripsData = sc.textFile("C://Data//data/trips.csv")
    val trips = tripsData.map(row => row.split(",", -1))

    val stationData = sc.textFile("C://Data//data/stations.csv")
    val stations = stationData.map(row => row.split(",", -1))

     val stationsIndexed = stations.keyBy(row => row(0).toInt)

        stationsIndexed.foreach(x => {
          print(x._1)
          x._2.foreach(y => {
            print(" ")
            print(y)
          })
          println
        })

    //    val tripsByStartTerminals = trips.keyBy(row => row(4).toInt)
    //
    //    tripsByStartTerminals.take(50).foreach(x => {
    //      print(x._1)
    //      x._2.foreach(y => {
    //        print(" ")
    //        print(y)
    //      })
    //      println
    //    })
    //
    //    val tripsByEndTerminals = trips.keyBy(row => row(7).toInt)
    //
    //    tripsByEndTerminals.take(50).foreach(x => {
    //      print(x._1)
    //      x._2.foreach(y => {
    //        print(" ")
    //        print(y)
    //      })
    //      println
    //    })
    //
    val stationsInternal = stations.mapPartitions(rows => rows.map(row => {
      parseStation(row)
    }))

    val tripsInternal = trips.mapPartitions(rows => rows.map(row => {
      parseTrip(row)
    }))
    val tripsByStartStation = tripsInternal.keyBy(record => record.startStation)
    val avgDurationByStartStation = tripsByStartStation
      .mapValues(x => x.duration)
      .groupByKey()
      .mapValues(col => col.reduce((a, b) => a + b) / col.size)
    time {
      avgDurationByStartStation.take(10).foreach(println)
    }
    println("+++++++++++++++++++++++")
    val avgDurationByStartStation2 = tripsByStartStation
      .mapValues(x => x.duration)
      .aggregateByKey((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(acc => acc._1 / acc._2)
    time {
      avgDurationByStartStation2.take(10).foreach(println)
    }
    val firstGrouped1 = tripsByStartStation
      .groupByKey()
      .mapValues(x =>
        x.toList.sortWith((trip1, trip2) =>
          trip1.startDate.compareTo(trip2.startDate) < 0))
    firstGrouped1.foreach(println)
    val firstGrouped2 = tripsByStartStation
      .reduceByKey((trip1, trip2) =>
        if (trip1.startDate.compareTo(trip2.startDate) < 0)
          trip1 else trip2)
    //    firstGrouped2.foreach(println)
    //
    //getBikeWithMaxDuration(tripsInternal)
    //
    //    getMaxDistBetweenStations(stationsInternal)
    //
     //   getBikePathWithMaxDuration(tripsInternal)
    //
    //    countBikes(tripsInternal)
    //
        getUsersWithMoreThan3HoursTrips(tripsInternal)

    sc.stop()

  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def parseStation(row: Array[String]): Station = {
    Station(
      stationId = row(0).toInt,
      name = row(1),
      lat = row(2).toDouble,
      long = row(3).toDouble,
      dockcount = row(4).toInt,
      landmark = row(5),
      installation = row(6)
    )
  }

  def parseTrip(row: Array[String]): Trip = {
    val timeFormat = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
    Trip(
      tripId = row(0).toInt,
      duration = row(1).toInt,
      startDate = LocalDateTime.parse(row(2), timeFormat),
      startStation = row(3),
      startTerminal = row(4).toInt,
      endDate = LocalDateTime.parse(row(5), timeFormat),
      endStation = row(6),
      endTerminal = row(7).toInt,
      bikeId = row(8).toInt,
      subscriptionType = row(9),
      zipCode = row(10)
    )
  }


  def getBikeWithMaxDuration(trips: RDD[Trip]): Int = {
    println("getBikeWithMaxDuration")
    val maxDurationBike = trips
      .map(trip => (trip.bikeId, trip.duration))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .first
    printf("Bike: %s, Duration: = %s\n", maxDurationBike._1, maxDurationBike._2)

    maxDurationBike._1
  }

  def dist(s1: Station, s2: Station): Double =
    sqrt(pow(s1.lat - s2.lat, 2) + pow(s1.long - s2.long, 2))

  def getMaxDistBetweenStations(stations: RDD[Station]): Unit = {
    println("getMaxDistBetweenStations")
    val maxDistStation = stations
      .cartesian(stations)
      .map(pair => (pair._1.stationId, pair._2.stationId, dist(pair._1, pair._2)))
      .sortBy(_._3, ascending = false)
      .first
    printf("From station №%s to station №%s dist: %f\n", maxDistStation._1, maxDistStation._2, maxDistStation._3)
  }

  def getBikePathWithMaxDuration(trips: RDD[Trip]): Unit = {
    print("getBikePathWithMaxDuration")

    val bike = getBikeWithMaxDuration(trips)
    trips
      .keyBy(trip => trip.bikeId)
      .lookup(bike)
      .sorted
      .foreach(
        trip => printf("%s -> %s \n", trip.startStation, trip.endStation)
      )
  }

  def countBikes(trips: RDD[Trip]): Unit = {
    println("countBikes")

    val count = trips
      .keyBy(trip => trip.bikeId)
      .groupByKey()
      .count

    printf("Count of bikes: %d\n", count)
  }

  def getUsersWithMoreThan3HoursTrips(trips: RDD[Trip]): Unit = {
    println("getUsersWithMoreThan3HoursTrips")

    trips
      .keyBy(trip => trip.zipCode)
      .mapValues(trip => trip.duration)
      .reduceByKey(_ + _)
      .filter(_._2 > 3 * 60 * 60)
      .foreach(
        v => printf("User with zip %s spent %s hours\n", v._1, v._2 / (360))
      )
  }

}


