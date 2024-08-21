package part3_types_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object a__Common_Types extends App {


  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))


  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter


  // multiple ways of filtering:
  moviesDF.select("Title").where(dramaFilter)


  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")


  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))


  /** Numbers
   */

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)


  /** Strings
   */

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))


  /** regex
   */
  val regexString = "volkswagen|vw"

  val volkswagenDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  volkswagenDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

}

