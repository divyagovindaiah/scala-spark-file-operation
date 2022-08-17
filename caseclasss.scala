import java.util.HashMap
import java.util.List

case class Telmetry(
  eid: String,
  progress:ProgressData,
  context: ContextDetails,
  edata: Edata,
  actor: Actor,
  `object`: ObjectDetails) {
  def getEid = eid

  def getUserId = actor.id

  def getPid = context.pdata.pid

  def getChannelId: String = context.channel
  def getProgress:Double = {
    var c = 0.0
    if (edata.summary.isInstanceOf[List[HashMap[String, String]]]) {
      edata.summary.forEach(x =>
        if (x.containsKey("progress"))
          c += x.get("progress").toDouble
      )
    }
    c
  }


  def getContentId: String = {
    if (`object`.isInstanceOf[ObjectDetails])
      `object`.id
    else "None"
  }
}

case class ContextDetails(
  sid: String,
  did: String,
  pdata: Pdata,
  channel: String)

case class Interrupt(
  userId: String,
  contentprogress: Int,
  playsCount: Int)

case class ProgressData(
  userId: String,
  progress: Double)

case class Actor(
`type`: String,
id: String)

case class Pdata(
  id: String,
  pid: String,
  ver: String)

case class Context(
  cdata: List[Actor],
  env: String,
  channel: String,
  pdata: Pdata,
  sid: String,
  did: String,
  rollup: Rollup)

case class Edata(
state: String,
props: List[String],
summary: List[HashMap[String,String]])

case class Rollup(
l1: String,
l2: String)

case class ObjectDetails(
  id: String,
  `type`: String,
  version: Double,
  rollup: Rollup)


case class OutputData(
  inProgressContent: Array[ProgressData])
