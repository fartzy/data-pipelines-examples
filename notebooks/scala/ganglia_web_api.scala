import scala.xml.Node
case class ClusterStats(metrics: Seq[GangliaMetric])
sealed trait GangliaMetric {
  def hostName: String
  def reportingTime: Long
  def metricName: String
}
case class DoubleGangliaMetric(hostName: String,
                               reportingTime: Long,
                               metricName: String,
                               metricVal: Double) extends GangliaMetric
case class LongGangliaMetric(hostName: String,
                             reportingTime: Long,
                             metricName: String,
                             metricVal: Long) extends GangliaMetric
class GangliaClient(endpoint: String = "http://localhost:8652") {
  def clusterStats(clusterName: String = "cluster"): ClusterStats = {
    // see https://stackoverflow.com/questions/14557546/is-there-a-api-for-ganglia
    // for info on the API and XML schema
    val doc = scala.xml.XML.load(new java.net.URL(s"${endpoint}/$clusterName"))
    val hostsStats = (doc \\ "HOST").flatMap { h =>
      val hostName = h.attribute("NAME").get.text
      val reportingTime = h.attribute("REPORTED").get.text.toLong
      (h \ "METRIC").collect {
        case m: Node if m.attribute("TYPE").exists(t => isDouble(t.text)) =>
          val metricName = m.attribute("NAME").map(_.text).getOrElse("???")
          val metricVal = m.attribute("VAL").map(_.text.toDouble).getOrElse(0.0)
          DoubleGangliaMetric(hostName, reportingTime, metricName, metricVal)
        case m: Node if m.attribute("TYPE").exists(t => isLong(t.text)) =>
          val metricName = m.attribute("NAME").map(_.text).getOrElse("???")
          val metricVal = m.attribute("VAL").map(_.text.toLong).getOrElse(0L)
          LongGangliaMetric(hostName, reportingTime, metricName, metricVal)
      }
    }
    ClusterStats(hostsStats)
  }
  private def isDouble(t: String) = (t == "double" || t == "float")
  private def isLong(t: String) = (t == "uint16" || t == "uint32")
}
