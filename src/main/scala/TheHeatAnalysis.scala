
import java.io.{BufferedWriter, File, FileWriter}

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.function.ForeachFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext._
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.joda.time.LocalDate
import spray.json._

import scala.collection.mutable.ListBuffer


object TheHeatAnalysis {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", sys.env("MONGO_URI"))
    .getOrCreate()

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Read in theheat_nowplaying
    val allSongsDF = MongoSpark.load(spark)
    // Convert allSongsDF to station local EST timezone
    val allSongsDF_EST = allSongsDF.select("*").withColumn("startTime", from_utc_timestamp(allSongsDF("startTime"), "GMT-05:00"))

    // Read in theheat_songs db
    val songsReadConfig = ReadConfig(Map("collection" -> "theheat_songs"),
      Some(ReadConfig(spark)))
    val songsInfoDF = MongoSpark.load(spark, songsReadConfig)


    //runSpotify(allSongsDF_EST)
    //runDataOverview(allSongsDF_EST)
    //runTopSongsAll(allSongsDF_EST, songsInfoDF)
    //runTopAlbumsAll(allSongsDF_EST)
    runSongsPerDay(allSongsDF_EST)

  }

  def runSpotify(allSongsDF : DataFrame) {
    val totalSongs = allSongsDF.count()
    println("Total Songs: " + totalSongs)

    val songsWithSpotify = allSongsDF.filter(allSongsDF("spotify.uri").notEqual(""))
    println("Songs with Spotify: " + songsWithSpotify.count())

    val songsWithoutSpotify = allSongsDF.filter(allSongsDF("spotify.uri").equalTo(""))
    println("Songs without Spotify: " + songsWithoutSpotify.count())

    //Show top artists without spotify
    songsWithoutSpotify.groupBy("artist", "song").count().sort(desc("count")).show(10)

    // Top 3 songs without spotify are Beyonce's, we can safely assume they are OK
    val beyonceSongs = songsWithoutSpotify.filter(songsWithoutSpotify("artist").equalTo("Beyonce"))
    val cleanSongs = songsWithSpotify.union(beyonceSongs)
    println("\nSongs with clean certain clean data : " + cleanSongs.count())

    // Save Data JSON
    case class SpotifySongCount(name: String, y: Long)
    object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val sscFormat = jsonFormat2(SpotifySongCount)
    }
    import MyJsonProtocol._
    import spray.json._
    val spotifyDataJson = Seq(new SpotifySongCount("Have Spotify Data", songsWithSpotify.count()),
      new SpotifySongCount("No Spotify Data", songsWithoutSpotify.count() - beyonceSongs.count()),
      new SpotifySongCount("Songs by Beyoncé", beyonceSongs.count()))
      .toJson

    val file = new File("results/spotifyData.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(spotifyDataJson.prettyPrint)
    bw.close()
  }

  def runDataOverview(allSongsDF : DataFrame) {
    // Oldest and Newest song recorded
    val oldestSong = allSongsDF.sort(asc("startTime")).limit(1).select("song", "artist", "startTime").collectAsList()
    println("Oldest Song: " + oldestSong.get(0))
    val newestSong = allSongsDF.sort(desc("startTime")).limit(1).select("song", "artist", "startTime").collectAsList()
    println("Newest Song: " + newestSong.get(0))

    val totalUniqueSongs = allSongsDF.groupBy("song", "artist").count().count()
    println("Total Unique Songs: " + totalUniqueSongs)

    val totalUniqueArtists = allSongsDF.groupBy("artist").count().count()
    println("Total Unique Artists: " + totalUniqueArtists)
  }

  def runTopSongsAll(allSongsDF : DataFrame, songInfoDF : DataFrame) {
    val topSongsRaw = allSongsDF.groupBy("song", "artist").count().sort(desc("count")).limit(50)
    val topSongs = topSongsRaw.join(songInfoDF, topSongsRaw("song") === songInfoDF("song")
      && topSongsRaw("artist") === songInfoDF("artist")).sort(desc("count")).collect()

    println("Top Songs (All)")
    var rank : Int = 1
    topSongs.foreach(row => {
      val song = row.get(0).toString
      val artist = row.get(1).toString
      val playCount = row.get(2)
      val spotify = row.getStruct(row.fieldIndex("spotify"))
      println("Rank: " + rank)
      println(artist + " - " + song + " - " + playCount)
      println(spotify.get(5))
      rank += 1
    })
  }

  def runTopAlbumsAll(allSongsDF : DataFrame) {
    // Top Albums (All)
    val topAlbums = allSongsDF.groupBy("spotify.album").count().sort(desc("count")).limit(50)
    topAlbums.show()
  }

  def runSongsPerDay(allSongsDF : DataFrame) {
    // Total songs played per day
    val totalSongsPerDay = allSongsDF.groupBy(year(allSongsDF("startTime")).alias("year"),
      month(allSongsDF("startTime")).alias("month"), dayofmonth(allSongsDF("startTime")).alias("day")).count()
      .sort("year", "month", "day")

    //Format data for Highcharts, add missing days as 0 and save to file.
    val startYear = Integer.parseInt(totalSongsPerDay.first().get(0).toString)
    val startMonth = Integer.parseInt(totalSongsPerDay.first().get(1).toString)
    val startDay = Integer.parseInt(totalSongsPerDay.first().get(2).toString)
    // End range
    val newestSongDate = totalSongsPerDay.sort(desc("year"), desc("month"), desc("day")).first()
    val endYear = Integer.parseInt(newestSongDate.get(0).toString)
    val endMonth = Integer.parseInt(newestSongDate.get(1).toString)
    val endDay = Integer.parseInt(newestSongDate.get(2).toString)
    // Iterate over whole data range, create empty DF and merge with existing counts
    def dayIterator(start: LocalDate, end: LocalDate) = Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end)

    val emptyDateList = new ListBuffer[Row]()
    dayIterator(new LocalDate(startYear, startMonth, startDay), new LocalDate(endYear, endMonth, endDay)).foreach(
      ts => {
        val row = Row(ts.getYear, ts.getMonthOfYear, ts.getDayOfMonth, 0)
        emptyDateList += row
      }
    )
    val emptyDateRDD = spark.sparkContext.parallelize(emptyDateList)

    val fieldsSchema = List(
      StructField("eYear", IntegerType, nullable = false),
      StructField("eMonth", IntegerType, nullable = false),
      StructField("eDay", IntegerType, nullable = false),
      StructField("eCount", IntegerType, nullable = false)
    )
    val emptyDateRange = spark.createDataFrame(emptyDateRDD, StructType(fieldsSchema))
    val mergedChartData = emptyDateRange.join(totalSongsPerDay, emptyDateRange("eYear") === totalSongsPerDay("year") &&
      emptyDateRange("eMonth") === totalSongsPerDay("month") && emptyDateRange("eDay") === totalSongsPerDay("day"), "left_outer")
      .withColumn("mergedCount", emptyDateRange("eCount") + totalSongsPerDay("count"))
      .sort("eYear", "eMonth", "eDay")

    // Prepare and save to file
    val stringOutput = StringBuilder.newBuilder
    stringOutput.append("[")
    mergedChartData.collect().foreach(r => {
      val year = Integer.parseInt(r.get(r.fieldIndex("eYear")).toString)
      val month = Integer.parseInt(r.get(r.fieldIndex("eMonth")).toString)
      val day = Integer.parseInt(r.get(r.fieldIndex("eDay")).toString)
      val count = r.get(r.fieldIndex("mergedCount"))
      stringOutput.append(String.format(f"[Date.UTC($year%d, $month%d, $day%d), $count%s],\n"))
    })
    stringOutput.append("]")

    val file = new File("results/songsPerDay.data")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(stringOutput.toString())
    bw.close()
  }

  def runSongsPerMonth(allSongsDF : DataFrame) {
    // Total songs per calendar month
    val totalSongsPerMonth = allSongsDF.groupBy(year(allSongsDF("startTime")).alias("year"),
      month(allSongsDF("startTime")).alias("month")).count().sort("year", "month")
  }

  def runMostPlayedSongOnAnyDay(allSongsDF : DataFrame) {
    // Most played song on any day
    val topPlayedSongAnyDay = allSongsDF.groupBy(year(allSongsDF("startTime")).alias("year"),
      dayofyear(allSongsDF("startTime")).alias("day"), allSongsDF("song"), allSongsDF("artist"))
      .count().sort(desc("count")).show()
  }

  def runTopPlayedSongPerMonth(allSongsDF : DataFrame) {
    /*
    // Most played song per month
    val songsPerMonth = allSongsDF.groupBy(year(allSongsDF("startTime")).alias("year"), month(allSongsDF("startTime")).alias("month"),
      allSongsDF("song"), allSongsDF("artist")).agg(count("*").alias("playedInMonth"))
    // Top 3 songs per month
    val top3PerMonth = songsPerMonth.withColumn("rank", rank().over(Window.partitionBy("year", "month")
      .orderBy(songsPerMonth("playedInMonth").desc)))
      .filter(col("rank") <= 3).sort("year", "month", "rank")
    top3PerMonth.show()
    // Top song per month
    val maxNumPlayedPerMonth = songsPerMonth.groupBy("year", "month").agg(max("playedInMonth").alias("maxInMonth"))
    val mostPlayedSongPerMonth = maxNumPlayedPerMonth.join(songsPerMonth, col("maxInMonth") === col("playedInMonth") &&
      maxNumPlayedPerMonth("year") === songsPerMonth("year") &&
      maxNumPlayedPerMonth("month") === songsPerMonth("month"))
    mostPlayedSongPerMonth.show()
    */
  }


}
