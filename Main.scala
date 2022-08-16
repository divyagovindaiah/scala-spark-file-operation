import scala.io.Source
import java.util.HashMap
import scala.collection.mutable
//import java.util.List
import java.io.BufferedReader
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import com.google.gson.Gson






  case class Telmetry(
                       eid: String,
                       progress:ProgressData,
                       context: ContextDetails,
                       edata: Edata,
                       actor: Actor,
                       completedcontent: Int,
                       objectData: ObjectBis,
                       channel: ChannelId,
                       contentid: ContentId

                     ) {
    def getId = eid

    def getUserId = actor.id

    def getPid = context.pdata.pid
  }


  case class ContextDetails(
                           sid: String,
                           did: String,
                           pdata: Pdata,
                         )


case class Interrupt(
                      userId: String,
                      contentprogress: Int,
                      playsCount: Int,

                    )

case class ProgressData(
                         userId: String,
                         contentId: String,
                         progress: Double
                       )
case class Actor(
                  `type`: String,
                  id: String
                )
case class Pdata(
                  id: String,
                  pid: String,
                  ver: String
                )
case class Context(
                    cdata: List[Actor],
                    env: String,
                    channel: String,
                    pdata: Pdata,
                    sid: String,
                    did: String,
                    rollup: Rollup
                  )
case class Edata(
                  state: String,
                  props: List[String],
                  `type`: String
                )
case class Rollup(
                   l1: String,
                   l2: String
                 )
case class ObjectBis(
                      id: String,
                      `type`: String,
                      version: Double,
                      rollup: Rollup
                    )
case class Flags(
                  ex_processed: Boolean
                )
case class ChannelId(
                  id:String
                  )
case class ContentId(
                    id:String
                    )
case class R00tJsonObject(
                           ver: String,
                           eid: String,
                           ets: Double,
                           actor: Actor,
                           context: Context,
                           edata: Edata,
                           `object`: ObjectBis,
                           mid: String,
                           syncts: Double,
                           `@timestamp`: String,
                           flags: Flags
                         )

//reading file
object Testing {

  def main(args: Array[String]): Unit = {
    //    CalculateTheStartAndEnd()
    readFile()


  }

  def CalculateTheStartAndEnd() = {
    val filename = "project.json"
    val fileSource = Source.fromFile(filename)
    var start: Int = 0;
    var end: Int = 0;
    var Interact: Int = 0;
    var Interrupt: Int = 0;


    for (line <- fileSource.getLines) {
      start = start + countOccurrences(line, "START:")
      end = end + countOccurrences(line, "END:")
      Interact = Interact + countOccurrences(line, "INTERACT:")
      Interrupt = Interrupt + countOccurrences(line, tgt = "INTERRUPT")
    }
    var myList = Array("{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}", "{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}", "{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}", "{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}")

    // Print all the array elements
    for (x <- myList) {
      println(x)
    }
    print("occurence of start in file", start)
    print("\n")
    print("occurence of end in file", end)
    print("...\n")
    print("occurence of INTERACT:", Interact)
    print("...\n")
    print("occurence of INTERRUPT:", Interrupt)
    fileSource.close()


  }
  def countOccurrences(src: String, tgt: String): Int =
    src.sliding(tgt.length).count(window => window == tgt)


  def readFile(): List[String] = {
    //    val gzipData = new GZIPInputStream(new BufferedInputStream(new FileInputStream("/home/sanctum/IdeaProjects/data-computation12/project.json")))
    //    val reader = new BufferedReader(new InputStreamReader(gzipData))
    //    val str = Iterator.continually(reader.readLine()).takeWhile(_ != null).toList
    // converted into list format
    val fileSource = Source.fromFile("/home/sanctum/IdeaProjects/data-computation12/project.json")
    val data = fileSource.getLines().toList
    var myList = Array("{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}", "{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}", "{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}", "{\n  \"ver\": \"3.0\",\n  \"eid\": \"AUDIT\",\n  \"ets\": 1606781788341,\n  \"actor\": {\n    \"type\": \"User\",\n    \"id\": \"8c31f821-000b-4100-b034-4b7c7eb56498\"\n  }\n}")






    println("readfile: ", data)

    data
  }

  def aggragate(line: String): Telmetry = {
    val gson = new Gson()
    val json = gson.fromJson(line, classOf[Telmetry])
    println("aggragate",json)
    json
  }

  def parseStream(reader: BufferedReader): List[Telmetry] =
    Iterator.continually(
      aggragate(reader.readLine())
    ).takeWhile(_ != null).toList


  def completed_content()(event: List[Telmetry], userId: String, contentId: String, ChannelId: String): Int = {

    event.filter(x => x.eid == "END" && x.context.pdata.pid == "sunbird.app.contentplayer" && x.getUserId == userId && x.objectData.id == contentId).size

  }
   List(ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002")).distinct
  val distinctList = List(ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002")).distinct
  val result=distinctList.size

 val output=distinctList.filter(_ => _.getChannelId == ChannelId)


}


