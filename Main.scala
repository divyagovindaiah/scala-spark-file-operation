/**
 * total number of completed contens by user
 * total number of distinct channel
 * total no of disctinct device id per channel
 * is content interrupted*/
import scala.io.Source
import java.util.HashMap
import java.io.{
  BufferedInputStream,
  BufferedReader,
  File,
  FileInputStream,
  FileNotFoundException,
  InputStreamReader,
  PrintWriter
}
import scala.collection.mutable
import java.io.BufferedReader
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import com.google.gson.Gson

object Testing {

  def main(args: Array[String]): Unit = {
    //   CalculateTheStartAndEnd()
  }

  val lines = readFile()
  val result: List[Telmetry] = lines.map((line) => aggregate(line))



  def inProgress(itr: List[Telmetry], endRange: Double, userId: String): Int =
    itr.count(x =>
      x.getProgress == endRange &&
        x.getUserId == userId)

  def getContentProgress(result: List[Telmetry]): Map[(String), Int] =
    result.groupBy(record => (record.getUserId)).map {
      case ((userId), logObjects) => (userId) ->
        inProgress(logObjects, 100.0, userId)
    }


  def toFile(data: OutputData, newFile: String) = {
    val gson = new Gson()
    val jsonData = gson.toJson(data)


    val fileWriter = new PrintWriter(
      new File(newFile)
    )
    try fileWriter.write(jsonData) finally fileWriter.close()
  }

  def Metrics(outToFile: Boolean, outPath: String): OutputData = {
    val result = readFile.map(x => aggregate(x))
    val progressContent = getContentProgress(result)
    val finalData = new OutputData(
      progressContent.map(
        // [(String), Int]
        r => new ProgressData(r._1, r._2).asInstanceOf[ProgressData]
      ).toList.toArray
    )
    if (outToFile) toFile(
      finalData,
      "/home/sanctum/IdeaProjects/data-computation12/src/main/scala/test.json"

    )
    finalData
  }


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


  def aggregate(line: String): Telmetry = {
    val gson = new Gson()
    val json = gson.fromJson(line, classOf[Telmetry])
    json
  }


}
 /*def CalculateTheStartAndEnd() =
  {
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


  def aggregate(line: String): Telmetry = {
    val gson = new Gson()
    val json = gson.fromJson(line, classOf[Telmetry])
    json
  }

  def parseStream(reader: BufferedReader): List[Telmetry] = {
    Iterator.continually(
      aggregate(reader.readLine())
    ).takeWhile(_ != null).toList

  }



  List(ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002")).distinct
  val distinctList = List(ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002"), ChannelId("0126684405014528002")).distinct
  val result = distinctList.size
  println(result)

*/











