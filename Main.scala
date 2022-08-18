/**
 * total number of completed contents by user
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
import com.typesafe.config.{ConfigFactory}


object TelemetryMetrics extends App {
  val gson = new Gson()
  val applicationConf = ConfigFactory.load("config.conf")
  val sourceFile = applicationConf.getString("app.sourceFile")
  val outputFile = applicationConf.getString("app.outputFile")

  /**
   * Loading Gzip file convert into buffer reader then in the form of list
   *
   * @return: returns list of strings
   */
  def readFile(): List[String] = {
    val gzipData = new GZIPInputStream(new BufferedInputStream(new FileInputStream(sourceFile)))
    val reader = new BufferedReader(new InputStreamReader(gzipData))
    val data = Iterator.continually(reader.readLine()).takeWhile(_ != null).toList
    data
  }

  /**
   * it can take the string and it can map the string to telemetryObjects
   *
   * @param line  : String of jsonObject
   * @return: retruns telemetryObjects
   */
  def aggregate(line: String): Telmetry = gson.fromJson(line, classOf[Telmetry])

  val telemetryObjects = readFile().map(line => aggregate(line))

  /**
   * This function can check the progress of content is 100.0 or not
   * Then returns number of content completed by a user
   *
   * @param result      : List of Telemetry Objects
   * @param progress  : compare the progress value
   * @param userId      : get the count by specific userId
   * @return: returns count of completed content by a user
   */
  def countCompleted(result: List[Telmetry], progress: Double, userId: String): Int =
    result.count(x =>
      x.getProgress == progress &&
        x.getUserId == userId)

  /**
   * it can  group the content those are the progress 100.0
   *
   * @param result  : contains list of telemetryObjects
   * @return: count the number of completed contents
   */
  def getContentProgress(result: List[Telmetry]): Map[String, Int] = {
    result.groupBy(record => (record.getUserId)).map {
      case (userId, logObjects) => userId ->
        countCompleted(logObjects, 100.0, userId)
    }

  }

  /**
   * it can writes the output to a file
   *
   * @param data  : it can takes outputdata
   */
  def toFile(data: OutputData) = {
    val jsonData = gson.toJson(data)
    val fileWriter = new PrintWriter(
      new File(outputFile)
    )
    try fileWriter.write(jsonData) finally fileWriter.close()
  }

  val completedContent = getContentProgress(telemetryObjects)

  val finalData = new OutputData(
    completedContent.map(
      // [String, Int]
      r => new ProgressData(r._1, r._2).asInstanceOf[ProgressData]
    ).toList.toArray)
  toFile(finalData)
}
