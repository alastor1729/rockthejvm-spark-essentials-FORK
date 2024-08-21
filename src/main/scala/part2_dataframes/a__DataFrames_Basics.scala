package part2_dataframes


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object a__DataFrames_Basics extends App {

  // creating a SparkSession:
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()


  // reading a DF:
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")


  // showing a DF:
  firstDF.show()
  firstDF.printSchema()


  // print rows from `firstDF`:
  firstDF.take(10).foreach(println)


  // manually creates the carsSchema
  val carsSchema =
    StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", DoubleType),
        StructField("Cylinders", LongType),
        StructField("Displacement", DoubleType),
        StructField("Horsepower", LongType),
        StructField("Weight_in_lbs", LongType),
        StructField("Acceleration", DoubleType),
        StructField("Year", StringType),
        StructField("Origin", StringType)
      )
    )


  // obtain a schema via Dataset "schema" function
  val carsDFSchema = firstDF.schema


  // read a DF with your "carsDFSchema"
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")


  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")


  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )


  /**
   * *
   *   Read movies.json from src/main/resources/data directory... and then:
   *   - print its schema
   *   - count the number of rows, call count()
   */
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(s"The Movies DF has ${moviesDF.count()} rows")

}

