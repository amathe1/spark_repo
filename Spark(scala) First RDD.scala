// Databricks notebook source
// Spark context is an entry point for any operation in Spark
// We need to create it when we are using Databricks approach (but when you use terminal Spark Context will be automaiclly connected)

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("Spark Basics").setMaster("local[*]")
val sc = SparkContext.getOrCreate(conf)

val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6))
rdd1.collect()

// Appyling basic transformation
val rdd2 = rdd1.map( x => x + 1)
rdd2.collect()

// Another way of applying transformation using lambda expression
val rdd3 = rdd1.map( _ + 1)
rdd3.collect()

// Multiple transformations
val rdd4 = rdd1.map(x => x + 1).map(x => x + 1).map(x => x + 1).map(x => x + 1).map(x => x + 1)
rdd4.collect()

// Getting partition information
rdd4.getNumPartitions

