import scala.io.Source;
object Main {
  def CalculateTheStartAndEnd() ={
    val filename = "project1.json"
    val fileSource = Source.fromFile(filename)
    var start:Int=0;
    var end:Int=0;
    var Interact:Int=0;

    for (line <- fileSource.getLines) {
      start=start+countOccurrences(line,"START:")
      end=end+countOccurrences(line,"END:")
      Interact=Interact+countOccurrences(line,"INTERACT:")
      print("...\n")
    }
    print("occurence of start in file",start)
    print("\n")
    print("occurence of end in file",end)
    print("...\n")
    print("occurence of INTERACT:",Interact)
    fileSource.close()
  }
  def countOccurrences(src: String, tgt: String): Int =
    src.sliding(tgt.length).count(window => window == tgt)
  def main(args:Array[String]):Unit={
    CalculateTheStartAndEnd()
  }

}