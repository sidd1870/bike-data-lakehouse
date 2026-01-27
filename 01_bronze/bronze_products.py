df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/bronze/raw_sources/source.crm/prd_info.csv")
)

df.write.mode("overwrite").saveAsTable("workspace.bronze.crm_prd_info")
