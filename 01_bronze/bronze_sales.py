df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/bronze/raw_sources/source.crm/sales_details.csv")
)

df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("workspace.bronze.crm_sales_details")
