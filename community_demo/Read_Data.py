# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
mount_point = dbutils.widgets.get("mount_point")
file_name = dbutils.widgets.get("file_name")

file_path = f"{mount_point}/{file_name}"
df = spark.read.csv(file_path, header=True)

df.show()
