
//Reading data from the source

val dataSrcDF = spark.read.parquet("/Desktop/data/test.parquet")
val extractDF = dataSrcDF.where(col("refund")====1).repartition(100)

//Transform data
// Writing custom function 1

def withStart()(df: DataFrame): DataFrame = {
df.withColumn("start", lit("hello Starting"))
}

// custom function 2
def withEnd()(df: DataFrame):DataFrame = {
df.withColumn("end",lit("goodbye"))
}

// model() function that chains the custom transformations.

df model()(df: DataFrame):DataFrame={
df
  .transform(withStart)
  .transform(withEnd)
}

// Load (or Report)
// we can use spark DataFrame writer to define a generic function that writes
// a DataFrame to a given location

def exampleWriter()(df: DataFrame):Unit = {
  val path = "//Desktop/sparkCode/example"
  df.write.mode(SaveMode.OverWrite).parquet(path)
}
