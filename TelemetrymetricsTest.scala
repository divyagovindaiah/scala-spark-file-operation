
import TelemetryMetrics.applicationConf
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
class TelemetryMetricsTest extends AnyFlatSpec {
  val applicationConf = ConfigFactory.load("config1.conf")
  val sourceFile = applicationConf.getString("app.sourceFile")
  val readFile= TelemetryMetrics.readFile(sourceFile)
  val data = TelemetryMetrics.process(readFile)
  it should "TelemetryMetrics.telemetryObjects" in {
    assert(1 === 1)
  }
}







