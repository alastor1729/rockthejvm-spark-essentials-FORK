package part3_types_datasets


import org.apache.spark.sql.{Dataset, SparkSession}


object b__Datasets_examples extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()


  /***
   *  ex_1) DataSet of a complex type for Cars:
   */

  // a) define Car case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )

  // b) read the DF from the file:
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")


  // c) define an encoder (importing the implicits):
  import spark.implicits._


  // d) convert the DataFrame to DataSet:
  val carsDS = carsDF.as[Car]

  // DS collection functions:
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()



  /***
   * ex_2) Joins with case classes:
   */
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  private case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  private case class Band(id: Long, name: String, hometown: String, year: Long)


  private val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  private val bandsDS = readDF("bands.json").as[Band]

  private val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDS.joinWith(
      bandsDS,
      guitarPlayersDS.col("band") === bandsDS.col("id"),
      "inner"
    )

  guitarPlayerBandsDS.show()

}

