package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._


class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("Country")
      .agg(
        sum("Number").alias("TotalNumber"),
        count("*").alias("Count"),
        max("Number").alias("MaxNumber"),
        min("Number").alias("MinNumber")
      )
  }
}
