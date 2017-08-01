package com.dev.spark_scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import org.apache.spark.SparkContext

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat._

object Ex1_1 {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Prepare Cancer Dataset").setMaster("local")
        val sc = new SparkContext(conf)

        // load data from local disk, for very large data sets use HDFS
        //
        val input = sc.textFile("/home/kmoh19/spark/crs1262/data/cancer-dataset/cancer.csv")

        // split rows into individual fields
        //
        val data = input.map(_.split(","))

        // remove rows where the seventh field has the value "?"
        //
        val noMissingValues = data.filter(_(6) != "?")
        val removedObservationCount = data.count - noMissingValues.count
        println(s">>> Number of observations removed = $removedObservationCount")

        // remove first (ID) field
        //
        val noIds = noMissingValues.map(_.drop(1))
        val numeric = noIds.map(_.map(_.toDouble))

        // map last column from 4 and 2 to "malignant" and "benign", respectively
        //
        val clearDiagnoses = numeric.map(x => x.patch(9, List(if (x(9) == 4) "malignant" else "benign"), 1))

        // examine summary statistics
        //
        val summaryStatistics = Statistics.colStats(numeric.map(Vectors.dense(_)))
        println(s"Count = ${summaryStatistics.count}")
        println(s"Minima = ${summaryStatistics.min}")
        println(s"Maxima = ${summaryStatistics.max}")
        println(s"# non-zeros = ${summaryStatistics.numNonzeros}")
        println(s"Variances = ${summaryStatistics.variance}")

        // join field values back into CSV strings
        //
        val output = clearDiagnoses.map(_.mkString(","))

        // save file back to local disk, for very large data sets use HDFS
        //
        output.saveAsTextFile("cancer-clean")
    }
}

