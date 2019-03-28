package io.animesh.dev

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkCSVJoinReader {


  def joinDF(leftDf: DataFrame, rightDf:DataFrame,leftCol :String,rightCol:String, joinType :String): Unit ={
    var joinDf = leftDf.join(rightDf,leftDf(leftCol) === rightDf(rightCol),joinType)
  }

  def readCSV(filepath:String ,sepOp : String = ";", isInferSchema : Boolean = true, isheader : Boolean = true,
              sparkSession :SparkSession): Unit ={
    var csvDF = sparkSession.read.format("csv")
      .option("sep", sepOp)
      .option("inferSchema", isInferSchema)
      .option("header", isheader)
      .load(filepath)
    csvDF
  }


  def main(args: Array[String]): Unit = {
    val user_file_path = "/usr/csvFiles/user.csv"
    val sparkSession =  SparkSession.builder().config("spark.executor.memory", "2g").appName("CSVDFapp").master("local[*]")
      .getOrCreate()

    var userDf = readCSV(user_file_path,sparkSession = sparkSession)

  }
}
