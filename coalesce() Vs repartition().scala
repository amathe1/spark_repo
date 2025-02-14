// Databricks notebook source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("Spark Basics").setMaster("local[*]")
val sc = SparkContext.getOrCreate(conf)

val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6))
rdd1.collect()
rdd1.getNumPartitions
rdd1.glom().collect()

val rdd2 = rdd1.coalesce(4, false)
rdd2.getNumPartitions
rdd2.glom().collect()

val rdd3 = rdd1.repartition(10)
rdd3.getNumPartitions
rdd3.glom().collect()
