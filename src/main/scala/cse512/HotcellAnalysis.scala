package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo = pickupInfo.select("x", "y", "z").where("x >= " + minX + " and y >= " + minY + " and z >= " + minZ + " and x <= " + maxX + " and y <= " + maxY + " and z <= " + maxZ).orderBy("z", "y", "x")
  var selectedHotCells = pickupInfo.groupBy("z", "y", "x").count().withColumnRenamed("count", "hotCells").orderBy("z", "y", "x")

  selectedHotCells.createOrReplaceTempView("selectedHotCells")

  val mean = (selectedHotCells.select("hotCells").agg(sum("hotCells")).first().getLong(0).toDouble) / numCells

  val standardDeviation = scala.math.sqrt((selectedHotCells.withColumn("squaredhotCell", pow(col("hotCells"), 2)).select("squaredhotCell").agg(sum("squaredhotCell")).first().getDouble(0) / numCells) - scala.math.pow(mean, 2))

  var adjacentCells = spark.sql("select hotCellTable_1.x as x, hotCellTable_1.y as y, hotCellTable_1.z as z, " + "sum(hotCellTable_2.hotCells) as noCell " + "from selectedHotCells as hotCellTable_1, selectedHotCells as hotCellTable_2 " + "where (hotCellTable_2.y = hotCellTable_1.y+1 or hotCellTable_2.y = hotCellTable_1.y or hotCellTable_2.y = hotCellTable_1.y-1) and (hotCellTable_2.x = hotCellTable_1.x+1 or hotCellTable_2.x = hotCellTable_1.x or hotCellTable_2.x = hotCellTable_1.x-1) and (hotCellTable_2.z = hotCellTable_1.z+1 or hotCellTable_2.z = hotCellTable_1.z or hotCellTable_2.z = hotCellTable_1.z-1)" + "group by hotCellTable_1.z, hotCellTable_1.y, hotCellTable_1.x " + "order by hotCellTable_1.z, hotCellTable_1.y, hotCellTable_1.x" )


  var adjCellNum = udf((inputX: Int, inputY: Int, inputZ: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) => HotcellUtils.calcAdjHotCell(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))
  var adjhotCell = adjacentCells.withColumn("adjacent_hot_cell", adjCellNum(lit(minX), lit(minY), lit(minZ), lit(maxX), lit(maxY), lit(maxZ), col("x"), col("y"), col("z")))

  var getisOrdScoreFunction = udf((numCells: Int , x: Int, y: Int, z: Int, adjacent_hot_cell: Int, noCell: Int, mean: Double, standardDeviation: Double) => HotcellUtils.calculateGscore(numCells, x, y, z, adjacent_hot_cell, noCell, mean, standardDeviation))
  var getisOrdScore = adjhotCell.withColumn("GetisOrdScore", getisOrdScoreFunction(lit(numCells), col("x"), col("y"), col("z"), col("adjacent_hot_cell"), col("noCell"), lit(mean), lit(standardDeviation))).orderBy(desc("GetisOrdScore")).limit(50)

  var finaloutput = getisOrdScore.select(col("x"), col("y"), col("z"))

  finaloutput.show()
  return finaloutput // YOU NEED TO CHANGE THIS PART
}
}
