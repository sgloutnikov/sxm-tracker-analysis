
import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.function.ForeachFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext._
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window


object TheHeatAnalysis {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", sys.env("MONGO_URI"))
      .getOrCreate()

    val allSongsDF = MongoSpark.load(spark)
    // Convert allSongsDF to station local EST timezone
    val allSongsDF_EST = allSongsDF.select("*").withColumn("startTime", from_utc_timestamp(allSongsDF("startTime"), "GMT-05:00"))
    //allSongsDF_EST.show()

    //printDataIntegrity(allSongsDF_EST)
    //printDataOverview(allSongsDF_EST)


    // Top Songs (All)
    //val topSongs = allSongsDF.groupBy("song", "artist").count().sort(desc("count")).limit(50)
    //topSongs.foreach(row => println(row))
    //println(topSongs.toJSON.show())

    // Top Albums (All)
    //val topAlbums = allSongsDF.groupBy("spotify.album").count().sort(desc("count")).limit(50)
    //topAlbums.show()
    //val topAlbums2 = allSongsDF.groupBy("album").count().sort(desc("count")).limit(50)
    //topAlbums2.show()


    // Total songs played per day
    //val totalSongsPerDay = allSongsDF_EST.groupBy(year(allSongsDF_EST("startTime")).alias("year"),
    //  dayofyear(allSongsDF_EST("startTime")).alias("day")).count().sort("year", "day").show()

    // Total songs per calendar month
    //val songsPerMonth = allSongsDF_EST.groupBy(year(allSongsDF_EST("startTime")).alias("year"),
    //  month(allSongsDF_EST("startTime")).alias("month")).count().sort("year", "month").show()

    // Most played song on any day
    //val topPlayedSongAnyDay = allSongsDF_EST.groupBy(year(allSongsDF_EST("startTime")).alias("year"), dayofyear(allSongsDF_EST("startTime")).alias("day"),
    //  allSongsDF_EST("song"), allSongsDF_EST("artist")).count().sort(desc("count")).show()


    // Most played song per month
//    val songsPerMonth = allSongsDF_EST.groupBy(year(allSongsDF_EST("startTime")).alias("year"), month(allSongsDF_EST("startTime")).alias("month"),
//      allSongsDF_EST("song"), allSongsDF_EST("artist")).agg(count("*").alias("playedInMonth"))
//    // Top 3 songs per month
//    val top3PerMonth = songsPerMonth.withColumn("rank", rank().over(Window.partitionBy("year", "month")
//      .orderBy(songsPerMonth("playedInMonth").desc)))
//      .filter(col("rank") <= 3).sort("year", "month", "rank")
//    top3PerMonth.show()
//    // Top song per month
//    val maxNumPlayedPerMonth = songsPerMonth.groupBy("year", "month").agg(max("playedInMonth").alias("maxInMonth"))
//    val mostPlayedSongPerMonth = maxNumPlayedPerMonth.join(songsPerMonth, col("maxInMonth") === col("playedInMonth") &&
//      maxNumPlayedPerMonth("year") === songsPerMonth("year") &&
//      maxNumPlayedPerMonth("month") === songsPerMonth("month"))
//    mostPlayedSongPerMonth.show()

    //val test = allSongsDF_EST.groupBy(year(allSongsDF_EST("startTime")).alias("year"), dayofyear(allSongsDF_EST("startTime")).alias("day")).count().sort("year", "day")
    //test.show()

  }

  def printDataIntegrity(allSongsDF : DataFrame) {
    val totalSongs = allSongsDF.count()
    println("Total Songs: " + totalSongs)

    val songsWithSpotify = allSongsDF.filter(allSongsDF("spotify.uri").notEqual(""))
    //val songsWithSpotify = allSongsDF.filter("spotify.uri != \"\"")
    println("Songs with Spotify: " + songsWithSpotify.count())

    val songsWithoutSpotify = allSongsDF.filter(allSongsDF("spotify.uri").equalTo(""))
    //val songsWithoutSpotify = allSongsDF.filter("spotify.uri = \"\"")
    println("Songs without Spotify: " + songsWithoutSpotify.count())

    //Show top artists without spotify
    songsWithoutSpotify.groupBy("artist", "song").count().sort(desc("count")).show(10)

    // Top 3 songs without spotify are Beyonce's, we can safely assume they are OK
    val beyonceSongs = songsWithoutSpotify.filter(songsWithoutSpotify("artist").equalTo("Beyonce"))
    val cleanSongs = songsWithSpotify.union(beyonceSongs)
    println("\nSongs with clean certain clean data : " + cleanSongs.count())
  }

  def printDataOverview(allSongsDF : DataFrame) {
    // Oldest and Newest song recorded
    //val oldestSong = allSongsDF.sort(asc("startTime")).limit(1).select("song", "artist", "startTime").collectAsList()
    //println("Oldest Song: " + oldestSong.get(0))
    //val newestSong = allSongsDF.sort(desc("startTime")).limit(1).select("song", "artist", "startTime").collectAsList()
    //println("Newest Song: " + newestSong.get(0))

    val totalUniqueSongs = allSongsDF.groupBy("song", "artist").count().count()
    println("Total Unique Songs: " + totalUniqueSongs)

    val totalUniqueArtists = allSongsDF.groupBy("artist").count().count()
    println("Total Unique Artists: " + totalUniqueArtists)
  }


}
