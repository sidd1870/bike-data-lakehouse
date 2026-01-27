df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/bronze/raw_sources/source.crm/cust_info.csv")
)

df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("workspace.bronze.crm_cust_info")
