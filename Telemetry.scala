package com.microsoft.spark.example
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_list, map_from_entries, struct}
import org.apache.spark.sql._
import scala.collection.immutable.HashMap
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`


case class TelemetryStructure(fields: Array[Telemetry])

case class Telemetry(
                        eid: String,
                        progress: ProgressData,
                        edata: EData,
                        actor: Actor)


case class Actor(
                  `type`: String,
                  id: String
                )
case class EData(
                  state: String,
                  props: List[String])
                  case class summary(
                            progress: Int)

case class ProgressData(
                         userId: String,
                         completed: Int
                       )



object Telemetry extends  App {

  def main(): Unit = {
    val sparksession = SparkSession.builder().config("spark.master", "local[*]").getOrCreate()
    computeProgress(sparksession)

  }

  def computeProgress(spark: SparkSession): Unit = {
    val schema = Encoders.product[Telemetry].schema
    val DataSet = spark.read.schema(schema).option("inferSchema", "true").json("/home/stpl/IdeaProjects/scala-sparkoperation/src/main/resources/Telemetrys.json")
    DataSet.show()

    /**
     * Count the total number of completed content by user
     */
   val data = DataSet.groupBy(DataSet("actor.id"))
    println(data)
 /*val CountCompleted = DataSet.groupBy("edata.summary.progress","actor.id").count()
   .filter(DataSet("eid") === "END")
   .agg(functions.map(
     col("edata.summary.progress"),
    map_from_entries(collect_list(struct(col("actor.id"), col("count"))))
   ).as("result")
  )
     .select("result")


   CountCompleted.show(true)*/

  }

  main()

}
