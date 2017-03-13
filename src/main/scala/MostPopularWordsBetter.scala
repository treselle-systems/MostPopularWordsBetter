package com.treselle.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object MostPopularWordsBetter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext 
     val conf = new SparkConf().setAppName("MostPopularWordsBetter").set("spark.hadoop.validateOutputSpecs", "false")  
	 val sc = new SparkContext(conf)
	
    // Read each line of the book into an RDD
    val lines = sc.textFile(args(0)) // RDD CREATED
    
    // Split using a regular expression that extracts words
    val words = lines.flatMap(x => x.split("\\W+").filter(x => x.matches("[A-Za-z]+") && x.length > 2)) 
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    
    // Save the file into HDFS.
    wordCountsSorted.saveAsTextFile(args(1))	
  }  
}

