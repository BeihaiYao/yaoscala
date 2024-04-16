package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  // Implements the original method from the trait with default CSV settings
  def read(path: String): DataFrame = {
    readWithSeparator(path, ",")
  }

  // Additional method to support custom separators
  def readWithSeparator(path: String, separator: String): DataFrame = {
    sparkSession
      .read
      .option("sep", separator)
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation'")
  }

}
