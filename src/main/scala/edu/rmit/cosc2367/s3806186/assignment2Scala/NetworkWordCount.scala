package edu.rmit.cosc2367.s3806186.assignment2Scala

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

object NetworkWordCount extends Logging {
  def main(args: Array[String]): Unit = {
    // Check if all parameter passed
    if (args.length < 4) {
      System.err.println("NetworkWordCount need 4 parameters")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val monitoringDirectory = args(0) // Set input folder
    val delayBatchTime = 10 // Wait for 10 seconds before checking directory

    logInfo("monitoringDirectory " + monitoringDirectory)
    logInfo("delayBatchTime " + delayBatchTime)

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local");
    val ssc = new StreamingContext(sparkConf, Seconds(delayBatchTime))

    // Create stream to monitor HDFS directory
    val directoryStream = ssc.textFileStream(monitoringDirectory)
    directoryStream.foreachRDD { fileRdd =>
      if (fileRdd.count() != 0) {
        // For each RDD run below tasks
        processTaskA(fileRdd, args(1))
        processTaskB(fileRdd, args(2))
        processTaskC(fileRdd, args(3))
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def processTaskA(fileRdd: RDD[String], opPath: String): Unit = {
    logInfo("Processing task a")
    println(opPath)
    // Remove special characters and cound word frequency
    val processed = fileRdd.flatMap(_.replaceAll("[^a-zA-Z0-9\\s+]", "").split("\\W+")).map(key => (key, 1)).reduceByKey(_ + _)
    processed.saveAsTextFile(opPath + System.nanoTime())
  }

  def processTaskB(fileRdd: RDD[String], opPath: String): Unit = {
    logInfo("Processing task b")
    println(opPath)
    // Remove special characters and filter words less than 5 characters
    val processed = fileRdd.flatMap(_.replaceAll("[^a-zA-Z0-9\\s+]", "").split("\\W+")).filter(x => x.length() < 5).distinct()
    processed.saveAsTextFile(opPath + System.nanoTime())
  }

  def processTaskC(fileRdd: RDD[String], opPath: String): Unit = {
    logInfo("Processing task a")
    println(opPath)

    val procLineContext = fileRdd.map(_.replaceAll("[^a-zA-Z0-9\\s+]", "").split("\\W+").toList)

    // Logic for word pair occurence frequency calculation
    val generatedPairs = procLineContext.map { indivLine =>
      for {
        i <- 0 until indivLine.length
        j <- 0 until indivLine.length
        if i != j
      } yield {
        // Create pair of current word and neighbour
        ((indivLine(i), indivLine(j)), 1)
      }
    }
    // Saving final result of task c
    generatedPairs.saveAsTextFile(opPath + System.nanoTime())
  }
}