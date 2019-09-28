
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

//EtlDefinition: Instantiate Etl Definition case class in spark-daria and use the process method to execute the ETL code

val etl = new EtlDefinition(
  soruceDF = extractDF,
  transform = model(),
  write = exampleWriter()
)

etl.prodess()


//EtlDefinition method signature

case Class EtlDefinition(
  soruceDF: DataFrame,
  transform: (DataFrame => DataFrame),
  write: (DataFrame => Unit)
  metadata: scala.collection.mutable.Map[String, Any]=
scala.collection.mutable.Map[String, Any]()
){
  def process(): Unit = {
    write(sourceDF.transform(transform))
}

// Multiple EtlDefinitions
val etls = scala.collection.mutable.Map[string, EtlDefinition]()
etls += ("bar" -> etl )
etls += ("bar" -> etl2 )
etls("bar").process()

/*
1> Make sure to repartition the DataFrame after filtering
2> Custom DataFrame transformations should be broken up, tested individually, and then chained in a model() method
3> Create EtlDefinition objects to organize your ETL logic and make sure all of your method signatures are correct
*/
