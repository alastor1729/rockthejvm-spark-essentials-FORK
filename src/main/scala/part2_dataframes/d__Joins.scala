package part2_dataframes


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr


object d__Joins extends App {


  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()


  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")


  // INNER JOINS
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show()


  /** LEFT OUTER join =>
   *             everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
   */
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")


  /** right outer join =>
   *        everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
   */
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")


  // OUTER JOIN = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")


  // [Last Example] using complex types
  guitaristsDF.join(
    guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)")
  ).show()


}

