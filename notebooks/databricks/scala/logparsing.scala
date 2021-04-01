// Databricks notebook source
// MAGIC %md
// MAGIC 66.249.69.97 - - [24/Sep/2014:22:25:44 +0000] "GET /071300/242153 HTTP/1.1" 404 514 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
// MAGIC 71.19.157.174 - - [24/Sep/2014:22:26:12 +0000] "GET /error HTTP/1.1" 404 505 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"
// MAGIC 71.19.157.174 - - [24/Sep/2014:22:26:12 +0000] "GET /favicon.ico HTTP/1.1" 200 1713 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"
// MAGIC 71.19.157.174 - - [24/Sep/2014:22:26:37 +0000] "GET / HTTP/1.1" 200 18785 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"
// MAGIC 71.19.157.174 - - [24/Sep/2014:22:26:37 +0000] "GET /jobmineimg.php?q=m HTTP/1.1" 200 222 "http://www.holdenkarau.com/" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"

// COMMAND ----------

import scala.util.matching.Regex.Groups
import scala.util.matching.Regex.Match
import java.text.SimpleDateFormat
import java.sql.Timestamp

val logPath = "/databricks-datasets/learning-spark/data-001/fake_logs"

case class Request(
 RequestType: String = "",
 RequestURL: String = "",
 RequestFormat: String = ""
)

case class LogRecord(
  ClientIP: String = "",
  Identity: String = "",
  Userid: String = "",
  TimeFinished: Timestamp = null,
  RequestLine: Request = Request(),
  ReturnStatusCode: Int = 0,
  Bytes: Int= 0,
  Referrers: String = "",
  UserAgents: String = ""
)


def convertToTimestamp(inTime: String) : java.sql.Timestamp = {
  val df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
  new Timestamp(df.parse(inTime).getTime);
}

def parseLogRecord: String => LogRecord = (inRecord: String) => {
  
  //pattern for the log records below 
  val pattern = """^([0-9.]+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] \"(.+?)\" (\d{3}) (\d+) \"([^\"]+)\" \"([^\"]+)\".*$""".r
  
  //pattern expression to parse the log record into a proper format
  val groups: Option[Match] = pattern.findFirstMatchIn(inRecord)
    groups match {
      case Some(gm) =>
       
        //Each group will be a diffierent field in the LogRecord case class.  
        val rt = s"clientIP: ${gm.group(1)} identity: ${gm.group(2)} userid: ${gm.group(3)} timeFinished: ${gm.group(4)} requestLine: ${gm.group(5)} statusCode: ${gm.group(6)} byteSize: ${gm.group(7)} listReferrers: ${gm.group(8)} listUserAgents: ${gm.group(9)}" 
     
        //Pre-processing some of the fields
        val rawReq =  gm.group(5).split(" ")
        val identity =  if (gm.group(2) == "-") "" else gm.group(2)
        val userID =  if (gm.group(3) == "-") "" else gm.group(3)
        val timeStamp = convertToTimestamp(gm.group(4))
        val request = Request(rawReq(0), rawReq(1), rawReq(2))
        val referrers = if (gm.group(8) == "-") "" else gm.group(8)
        
        LogRecord(gm.group(1)
          , identity
          , userID
          , timeStamp
          , request
          , gm.group(6).toInt
          , gm.group(7).toInt
          , referrers
          , gm.group(9))
      case None => LogRecord()
    }
} //end parseLogRecord

val logRecordDS = spark.read.text(logPath).as[String]
  .map(l => parseLogRecord(l))

display(logRecordDS)