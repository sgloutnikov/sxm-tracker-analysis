
import java.io.{BufferedWriter, File, FileWriter}

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.function.ForeachFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext._
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import spray.json._


object TheHeatAnalysis {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", sys.env("MONGO_URI"))
      .getOrCreate()

    // Read in theheat_nowplaying
    val allSongsDF = MongoSpark.load(spark)
    // Convert allSongsDF to station local EST timezone
    val allSongsDF_EST = allSongsDF.select("*").withColumn("startTime", from_utc_timestamp(allSongsDF("startTime"), "GMT-05:00"))

    // Read in theheat_songs db
    val songsReadConfig = ReadConfig(Map("collection" -> "theheat_songs"),
      Some(ReadConfig(spark)))
    val songsInfoDF = MongoSpark.load(spark, songsReadConfig)


    runSpotify(allSongsDF_EST)
    //runDataOverview(allSongsDF_EST)
    //runTopSongsAll(allSongsDF_EST, songsInfoDF)
    //runTopAlbumsAll(allSongsDF_EST)

    //processTop(allSongsDF_EST)
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

  def processTop(allSongsDF : DataFrame) {
    /*
    // Total songs played per day
    val totalSongsPerDay = allSongsDF.groupBy(year(allSongsDF("startTime")).alias("year"),
      dayofyear(allSongsDF("startTime")).alias("day")).count().sort("year", "day").show()

    // Total songs per calendar month
    val totalSongsPerMonth = allSongsDF.groupBy(year(allSongsDF("startTime")).alias("year"),
      month(allSongsDF("startTime")).alias("month")).count().sort("year", "month").show()

    // Most played song on any day
    val topPlayedSongAnyDay = allSongsDF.groupBy(year(allSongsDF("startTime")).alias("year"),
      dayofyear(allSongsDF("startTime")).alias("day"), allSongsDF("song"), allSongsDF("artist"))
      .count().sort(desc("count")).show()


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
