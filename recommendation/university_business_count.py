# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, regexp_replace
import subprocess
import sys

def createSparkSession():
    return SparkSession.builder \
        .appName("University Business Classification Count") \
        .getOrCreate()

def rename_output_file(temp_path, final_name):
    try:
        find_cmd = "hdfs dfs -ls {0} | grep .csv".format(temp_path) 
        result = subprocess.check_output(find_cmd, shell=True).decode()
        csv_file = result.strip().split()[-1]
                                        
        mv_cmd = "hdfs dfs -mv {0} {1}".format(csv_file, final_name)
        subprocess.check_output(mv_cmd, shell=True)
                                                                                
        rm_cmd = "hdfs dfs -rm -r {0}".format(temp_path)
        subprocess.check_output(rm_cmd, shell=True)
                                                                      
    except subprocess.CalledProcessError as e:
        print("Error renaming file: {0}".format(e))

def dataLoad(spark):
    file_path = "hdfs:///user/maria_dev/term_project/input/restaurant_with_universities.csv"

    df = spark.read.option("header", "true").csv(file_path)
    df = df.withColumn("대학교", regexp_replace(col("대학교"), '"', ""))
    df = df.withColumn("대학교", explode(split(col("대학교"), ", ")))
    return df

def preprocessing(df):
    temp_path = "hdfs:///user/maria_dev/term_project/temp_classification"
    final_name = "hdfs:///user/maria_dev/term_project/output/classification_codes.csv"
    
    classification_df = df.select(
        "상권업종대분류코드",
        "상권업종대분류명",
        "상권업종중분류코드",
        "상권업종중분류명",
        "상권업종소분류코드",
        "상권업종소분류명"
    ).distinct()

    classification_df = classification_df.orderBy("상권업종중분류코드", "상권업종소분류코드") 
    classification_df.coalesce(1).write.mode("overwrite").csv(temp_path, header=True)
    
    rename_output_file(temp_path, final_name)

    result_df = df.select(
        "상권업종중분류코드",
        "상권업종소분류코드",
        "대학교"
    )

    return result_df

def code_count(df):
    temp_path = "hdfs:///user/maria_dev/term_project/temp_count"
    final_name = "hdfs:///user/maria_dev/term_project/output/university_business_count.csv"

    result_df = df \
        .groupBy("대학교", "상권업종중분류코드", "상권업종소분류코드") \
        .agg(count("*").alias("count")) \
        .orderBy("대학교", "상권업종중분류코드", "상권업종소분류코드")

    result_df.coalesce(1).write.mode("overwrite").csv(temp_path, header=True)
    rename_output_file(temp_path, final_name)
    
    return result_df

def main():
    spark = createSparkSession()
    initial_df = dataLoad(spark)
    processed_df = preprocessing(initial_df)

    processed_df.printSchema()
    processed_df.show(truncate=False)

    result_df = code_count(processed_df)
    result_df.show(truncate=False)
    
    spark.stop()


if __name__=="__main__":
    main()





