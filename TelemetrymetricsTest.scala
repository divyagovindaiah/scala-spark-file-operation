
import org.scalatest.funsuite.AnyFunSuite
class TelemetryMetricsTest extends AnyFunSuite {
  val telemetryObjects = TelemetryMetrics.telemetryObjects
  val readfile = TelemetryMetrics.readFile()
  val aggregate = TelemetryMetrics.aggregate()
  val completedContent = TelemetryMetrics.completedContent

  println(telemetryObjects)
  test("TelemetryMetrics.countCompleted") {
    assert(TelemetryMetrics.countCompleted(telemetryObjects, 100.0, "80977d17-76ef-4507-b66c-11d4e5c2f045") == 1)
  }
}







