val df = spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("nullValue","NA")
  .option("timeStampFormat","yyyy-MM-dd'T'HH:mm:ss")
  .option("mode","failfast")
  .option("path","/Users/artatrana/Desktop/MediaMartSaturn/MMS-Challenge/data.csv")
  .load()

  val df = df.select($"Gender",$"treatement")
  val df2 = df.select($"Gender",
    (when ($"treatment" === "Yes",1).otherwise(0)).allias("All-Yes"),
    (when($"treatment" === "No",1).otherwise(0)).allias("All-No")
  )

  val d3 = df2.groupBy("Gender").agg(sum($"All-Yes"),sum($"All-No"))
  df3.show

  /*--------------------
  We can write a UDF in sala function and register it
  def parseGender(g: String):={
    g.toLowerCase match{
      case "male" |"m" |"maile" |
          "mal" | "male (cis)" | "make" | "male"
          | "man" | "msle" | "mail" | "malr"
          | "cis man" | " cis male" => "Male"
      case "cis female" | "f" | "female" |
          "women" | " femake" | "fmail" => "Fmale"
      case _ => "Transgender"
    }
  }

  val parseGernderUDF = udf(parseGender _)
  --------------*/

  val df3 = df2.select(parseGenderUDF($"Gender").alias("Gender"),
                       $"All-Yes",
                       $"All-No")

  val df4 = df3.groupBy("Gender").agg(sum($"All-Yes"), sum($"All-No"))
  val df5 = df4.filter($"Gender" =!= "Transgender")
