/**
 * total number of completed contents by user
 * total number of distinct channel
 * total no of disctinct device id per channel
 * is content interrupted*/

import java.io.{
  BufferedInputStream,
  BufferedReader,
  File,
  FileInputStream,
  InputStreamReader,
  PrintWriter
}
import java.util.zip.GZIPInputStream
import com.google.gson.Gson
import com.typesafe.config.{ConfigFactory}


object TelemetryMetrics extends App {
  val gson = new Gson()
  val applicationConf = ConfigFactory.load("config.conf")
  val sourceFile = applicationConf.getString("app.sourceFile")
  val outputFile = applicationConf.getString("app.output.totalContentOutFile")
  val metricsOutFile = applicationConf.getString("app.output.metricsOutFile")

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
   * @param line : String of jsonObject
   * @return: retruns telemetryObjects
   */
  def aggregate(line: String): Telmetry = gson.fromJson(line, classOf[Telmetry])

  val telemetryObjects = readFile().map(line => aggregate(line))

  /**
   * This function can check the progress of content is 100.0 or not
   * Then returns number of content completed by a user
   * @param result   : List of Telemetry Objects
   * @param progress : compare the progress value
   * @param userId   : get the count by specific userId
   * @return: returns count of completed content by a user
   */
  def countCompleted(result: List[Telmetry], progress: Double, userId: String): Int = {
    result.count(x =>
      x.getProgress == progress &&
        x.getUserId == userId)
  }

  /**
   * in this count the number of distcint channel
   * */
  def distinctChannels(result1: List[Telmetry], channel: String): Int = {
    result1.count(x => x.getChannelId == channel)
  }

  /**
   * it can  group the content those are the progress 100.0
   *
   * @param result : contains list of telemetryObjects
   * @return: count the number of completed contents
   */
  def getContentProgress(result: List[Telmetry]): Map[String, Int] = {
    result.groupBy(record => (record.getUserId)).map {
      case (userId, logObjects) => userId ->
        countCompleted(logObjects, 100.0, userId)
    }

  }

  /**
   * it can group the channalls are distinct
   * @param result: contains List of telemetry objects
   * @return: find out the number of disctinctchannnel
   */
  def getUniqueChannel(result: List[Telmetry]): Map[String, Int] = {
    //val groupByData = result.groupBy(f => f.getChannelId)
    val metrics = result.groupBy(f => f.getChannelId).map(f => (f._1, f._2.size))
    metrics
  }

  /**
   * it can writes the output to a file
   *@param data : it can takes outputdata
   */
  def toFile(data: OutputData) = {
    val jsonData = gson.toJson(data)
    val fileWriter = new PrintWriter(
      new File(outputFile)
    )
    try fileWriter.write(jsonData) finally fileWriter.close()
  }

  /**
   *This function is used to write the obtained metrics to a new json file
   *@param data it can takes outputData1
   */

  def toFile1(data: OutputData1) = {
    val jsonData = gson.toJson(data)
    val fileWriter = new PrintWriter(
      new File(metricsOutFile)

    )
    try fileWriter.write(jsonData) finally fileWriter.close()
  }


  val completedContent = getContentProgress(telemetryObjects)
  val distinctChannels = getUniqueChannel(telemetryObjects)
  val finalData = new OutputData(
    completedContent.map(
      r => new ProgressData(r._1, r._2).asInstanceOf[ProgressData]
    ).toList.toArray)
  toFile(finalData)
  distinctChannels.map(f => println("data", f))
  val finalData1 = new OutputData1(
    distinctChannels.map(
    r =>  new channel(r._1,r._2).asInstanceOf[channel]
    ).toArray)
    toFile1(finalData1)


}
