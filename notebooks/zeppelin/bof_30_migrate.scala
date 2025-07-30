import java.lang.Integer
import java.sql.{Date, Timestamp}
import java.text.{DateFormat, SimpleDateFormat}
import java.time.LocalDate
import java.util.{Calendar, GregorianCalendar, Locale, Random}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}

import org.joda.time.format.DateTimeFormat

import spark.implicits._
import sys.process._

class BofParser {

  def loadSchema(inKeys: String, inAllFieldsInOrder: String): (List[String], List[String]) = {
    (
      inKeys.split(",").toList,
      inAllFieldsInOrder.split(",").toList
    )
  }

  def schemaMapper: (String, List[String]) => StructField = (fieldName: String, keys: List[String]) =>
    fieldName match {
      case x if keys.contains(x) => StructField(fieldName, StringType, nullable = false)
      case _                     => StructField(fieldName, StringType, nullable = false)
    }

  def joinIngestBofs(oldDF: DataFrame, finalDF: DataFrame, keys: Array[String]): Try[DataFrame] = Try {
    val joinExprs = keys
      .map(key => oldDF(key) <=> finalDF(key))
      .reduce(_ && _)

    oldDF
      .join(finalDF, joinExprs, "outer")
      .select(
        (oldDF.columns ++ finalDF.columns).distinct.map { colName =>
          if (!oldDF.columns.contains(colName)) finalDF.col(colName)
          else if (!finalDF.columns.contains(colName)) oldDF.col(colName)
          else coalesce(finalDF.col(colName), oldDF.col(colName)).as(colName)
        }: _*
      )
  }

  /***********************************************************
   * Example functions for processing data
   ***********************************************************/

  def selectBmilsvc1(
      bdodaacDF: DataFrame,
      bcmilDF: DataFrame,
      byCUST_GRP3andBDODAACorderedByBCNFMILSP: WindowSpec
  ): Try[DataFrame] = Try {
    bdodaacDF
      .join(
        bcmilDF,
        when(bcmilDF("BSERVICE1") === "*", true)
          .otherwise(bcmilDF("BSERVICE1") === bdodaacDF("BDODAAC").substr(1, 1)) &&
        when(bcmilDF("BSERVICE2") === "*", true)
          .otherwise(bcmilDF("BSERVICE2") === bdodaacDF("BDODAAC").substr(2, 1)) &&
        when(bcmilDF("BSERVICE3") === "*", true)
          .otherwise(bcmilDF("BSERVICE3") === bdodaacDF("BDODAAC").substr(3, 1)) &&
        when(bcmilDF("BFMS_IND") === "*", true)
          .otherwise(
            bcmilDF("BFMS_IND") === when(isnull(bdodaacDF("CUST_GRP3")), lit("N")).otherwise(lit("Y"))
          ) &&
        bcmilDF("BMILGP1FG") === lit("Y"),
        "left"
      )
      .withColumn("BMILGP1FG_RULE_RANK", rank().over(byCUST_GRP3andBDODAACorderedByBCNFMILSP))
      .filter($"BMILGP1FG_RULE_RANK" === lit(1))
      .withColumnRenamed("BMILGP1RS", "BMILSVC_1")
      .select((bdodaacDF.columns :+ "BMILSVC_1").map(expr): _*)
  }

  def selectBmilsvc2(
      bdodaacDF: DataFrame,
      bcmilDF: DataFrame,
      byCUST_GRP3andBDODAACorderedByBCNFMILSP: WindowSpec
  ): Try[DataFrame] = Try {
    bdodaacDF
      .join(
        bcmilDF,
        when(bcmilDF("BSERVICE1") === "*", true)
          .otherwise(bcmilDF("BSERVICE1") === bdodaacDF("BDODAAC").substr(1, 1)) &&
        when(bcmilDF("BSERVICE2") === "*", true)
          .otherwise(bcmilDF("BSERVICE2") === bdodaacDF("BDODAAC").substr(2, 1)) &&
        when(bcmilDF("BSERVICE3") === "*", true)
          .otherwise(bcmilDF("BSERVICE3") === bdodaacDF("BDODAAC").substr(3, 1)) &&
        when(bcmilDF("BFMS_IND") === "*", true)
          .otherwise(
            bcmilDF("BFMS_IND") === when(isnull(bdodaacDF("CUST_GRP3")), lit("N")).otherwise(lit("Y"))
          ) &&
        bcmilDF("BMILGP2FG") === lit("Y"),
        "left"
      )
      .withColumn("BMILGP2FG_RULE_RANK", rank().over(byCUST_GRP3andBDODAACorderedByBCNFMILSP))
      .filter($"BMILGP2FG_RULE_RANK" === lit(1))
      .withColumnRenamed("BMILGP2RS", "BMILSVC_2")
      .select((bdodaacDF.columns :+ "BMILSVC_2").map(expr): _*)
  }

  def selectBmilsvc3(
      borgdnumDF: DataFrame,
      bcmilDF: DataFrame,
      custDF: DataFrame,
      byCUST_GRP3andBDODAACandSOLD_TOorderedByBCNFMILSP: WindowSpec
  ): Try[DataFrame] = Try {
    val custSubDF = custDF
      .filter(col("CUSTGRP3") === lit("FMS") && col("OBJVERS") === lit("A"))
      .select(col("CUSTOMER"))
      .distinct()

    borgdnumDF
      .join(custSubDF, custSubDF("CUSTOMER") <=> borgdnumDF("SOLD_TO"), "left")
      .select(
        $"*",
        when($"BORGDNUM".substr(1, 1).isin("B", "D", "K", "P", "T") && !isnull(col("CUSTOMER")), lit(""))
          .alias("BMILSVC_3_BLANKED")
      )
      .join(
        bcmilDF,
        when(bcmilDF("BSERVICE1") === "*", true)
          .otherwise(bcmilDF("BSERVICE1") === borgdnumDF("BORGDNUM").substr(1, 1)) &&
        when(bcmilDF("BSERVICE2") === "*", true)
          .otherwise(bcmilDF("BSERVICE2") === borgdnumDF("BORGDNUM").substr(2, 1)) &&
        when(bcmilDF("BSERVICE3") === "*", true)
          .otherwise(bcmilDF("BSERVICE3") === borgdnumDF("BORGDNUM").substr(3, 1)) &&
        when(bcmilDF("BFMS_IND") === "*", true)
          .otherwise(
            bcmilDF("BFMS_IND") === lit("Y") && borgdnumDF("CUST_GRP3") === lit("FMS")
          ) &&
        bcmilDF("BMILGP3FG") === lit("Y"),
        "left"
      )
      .withColumn(
        "BMILGP3FG_RULE_RANK",
        rank().over(byCUST_GRP3andBDODAACandSOLD_TOorderedByBCNFMILSP)
      )
      .filter($"BMILGP3FG_RULE_RANK" === lit(1))
      .withColumn("BMILSVC_3", coalesce(col("BMILGP3RS"), col("BMILSVC_3_BLANKED")))
      .select((borgdnumDF.columns :+ "BMILSVC_3").map(expr): _*)
  }

  def selectBstagrp(
      mainBof12DF: DataFrame,
      BCNFSTAGPDF: DataFrame
  ): Try[DataFrame] = Try {
    mainBof12DF
      .join(
        BCNFSTAGPDF,
        when(BCNFSTAGPDF("DOC_TYPE") === "*", true)
          .otherwise(BCNFSTAGPDF("DOC_TYPE") === mainBof12DF("DOC_TYPE")),
        "left"
      )
      .drop(mainBof12DF("DOC_TYPE"))
      .withColumn("BSTAGRP", BCNFSTAGPDF("BCNFCHR01"))
      .select((mainBof12DF.columns :+ "BSTAGRP").map(expr): _*)
  }

  val getFiscalYYYYMM: String => String = (inDate: String) => {
    try {
      val sdf = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH)
      val date = sdf.parse(inDate)
      val cal = Calendar.getInstance()
      cal.setTime(date)

      val month = if (cal.get(Calendar.MONTH) < 9) s"${cal.get(Calendar.MONTH) + 4}" else s"${cal.get(Calendar.MONTH) - 8}"
      val year = if (cal.get(Calendar.MONTH) >= 9) s"${cal.get(Calendar.YEAR) + 1}" else s"${cal.get(Calendar.YEAR)}"

      val yearString = f"$year%04d"
      val monthString = f"$month%02d"

      s"$yearString$monthString"
    } catch {
      case _: Throwable => null
    }
  }

  val getFiscalYYYYMMUDF = udf[String, String](getFiscalYYYYMM)

  def calculateBactShip: Column = {
    when(col("BCNFCHR01") === lit("L"), col("LOAD_DATE"))
      .otherwise(
        when(col("BCNFCHR01") === lit("G"), col("BGR_DATE"))
          .otherwise(
            when(col("BCNFCHR01") === lit("C"), col("CONF_DATE"))
              .otherwise(
                when(col("BCNFCHR01") === lit("P"), col("BCUSTPODT"))
                  .otherwise(
                    when(col("BCNFCHR01") === lit("M"), col("BMATRCVDT"))
                      .otherwise(
                        when(
                          col("BCNFCHR01") === lit("O") && length(col("BCNFCHR01")) < lit(1),
                          julianDateParseUDF(col("BMATRCVDT").substr(14, 4))
                        )
                        .otherwise(
                          when(col("BCNFCHR01") === lit("B"), lit(""))
                            .otherwise(lit(""))
                        )
                      )
                  )
              )
          )
      )
  }

  // Helper functions for BTGT_SHIP calculation
  def calculateBtgtShipForItemCategories: Column = {
    when(
      col("ITEM_CATEG").isin(rSalesDocItemCat1Line) && 
      length(col("GV_CREATED_ON")) > lit(0),
      addDays(col("GV_CREATED_ON"), lit(365))
    )
  }

  def calculateBtgtShipForDocType1: Column = {
    when(
      col("DOC_TYPE").isin(rSalesDocType1Line) && 
      length(col("GV_CREATED_ON")) > lit(0),
      addDays(col("GV_CREATED_ON"), lit(15))
    )
  }

  def calculateBtgtShipForDocType2: Column = {
    when(
      col("DOC_TYPE").isin(rSalesDocType2Line) && 
      col("PROFIT_CTR").substr(3, 2) === lit("21") && 
      length(col("GV_CREATED_ON")) > lit(0),
      addDays(col("GV_CREATED_ON"), lit(6))
    )
    .otherwise(
      when(
        col("DOC_TYPE").isin(rSalesDocType2Line) && 
        length(col("BZZESD")) > lit(0) && 
        length(col("GV_CREATED_ON")) > lit(0) && 
        col("BZZESD") >= addDays(col("GV_CREATED_ON"), lit(2)),
        col("BZZESD")
      )
      .otherwise(
        when(
          col("DOC_TYPE").isin(rSalesDocType2Line) && 
          length(col("BZZESD")) > lit(0) && 
          length(col("GV_CREATED_ON")) > lit(0) && 
          col("DSDEL_DATE") < addDays(col("GV_CREATED_ON"), lit(2)),
          addDays(col("GV_CREATED_ON"), lit(2))
        )
        .otherwise(
          when(
            col("DOC_TYPE").isin(rSalesDocType2Line) && 
            length(col("DSDEL_DATE")) > lit(0) && 
            length(col("GV_CREATED_ON")) > lit(0) && 
            col("DSDEL_DATE") >= addDays(col("GV_CREATED_ON"), lit(2)),
            col("DSDEL_DATE")
          )
          .otherwise(
            when(
              col("DOC_TYPE").isin(rSalesDocType2Line) && 
              length(col("DSDEL_DATE")) > lit(0) && 
              length(col("GV_CREATED_ON")) > lit(0) && 
              col("DSDEL_DATE") < addDays(col("GV_CREATED_ON"), lit(2)),
              addDays(col("GV_CREATED_ON"), lit(2))
            )
            .otherwise(
              when(
                col("DOC_TYPE").isin(rSalesDocType2Line) && 
                length(col("GV_CREATED_ON")) > lit(0),
                addDays(col("GV_CREATED_ON"), lit(15))
              )
            )
          )
        )
      )
    )
  }

  def calculateBtgtShipForFmsLogic: Column = {
    when(
      col("CUST_GRP3") === lit("FMS") && 
      col("BFMS_PRG").isin(rFmsProgCdLine) && 
      col("BADMLDTME") > lit(0) && 
      col("BPRODLDTM") > lit(0) && 
      length(col("BPRODLDTM")) > lit(0),
      date_format(
        date_add(
          from_unixtime(
            unix_timestamp(col("GV_CREATED_ON"), "yyyyMMdd"),
            "yyyy-MM-dd"
          ),
          col("BADMLDTME") + col("BPRODLDTM")
        ),
        "yyyyMMdd"
      )
    )
    .otherwise(
      when(
        col("PROFIT_CTR") >= col("BLOPRCTR") && 
        col("PROFIT_CTR") <= col("BHIPRCTR") && 
        col("BFMS_PRG").isin(rFmsProgCdLine) && 
        col("CUST_GRP3") === lit("FMS"),
        date_format(
          date_add(
            from_unixtime(
              unix_timestamp(col("GV_CREATED_ON"), "yyyyMMdd"),
              "yyyy-MM-dd"
            ),
            col("BMM_PLT")
          ),
          "yyyyMMdd"
        )
      )
      .otherwise(
        when(
          col("BFMS_PRG").isin(rFmsProgCdLine) && 
          col("CUST_GRP3") === lit("FMS"),
          addDays(col("GV_CREATED_ON"), lit(180))
        )
      )
    )
  }

  def calculateBtgtShipForSpecialCases: Column = {
    when(
      col("BOFBNSRDD").substr(2, 2).isin(isNddExtendedList) && 
      col("BOFBNSRDD").substr(1, 1) === lit("S"),
      getLastDoMUdf(col("BOFBNSRDD"))
    )
    .otherwise(
      when(
        col("BORD_TYPE") === lit("C") && 
        col("BACQADVCD").isin(rAac1Line) && 
        col("BOFPRICD").isin(rHighPriorityLine),
        addDays(col("GV_CREATED_ON"), lit(3))
      )
      .otherwise(
        when(
          ((col("BORD_TYPE") === lit("C") && 
            col("BACQADVCD").isin(rAac1Line) && 
            col("BOFBNSRDD").substr(1, 1).isin(rExpNsRdd2Line)) || 
           col("BOFBNSRDD").isin(rExpNsRdd1Line)) && 
          col("BOFPRICD").isin(rLowPriorityLine),
          addDays(col("GV_CREATED_ON"), lit(3))
        )
        .otherwise(
          when(
            ((col("BORD_TYPE") === lit("C") && 
              col("BACQADVCD").isin(rAac1Line) && 
              !col("BOFBNSRDD").substr(1, 1).isin(rExpNsRdd2Line)) && 
             !col("BOFBNSRDD").isin(rExpNsRdd1Line)) && 
            col("BOFPRICD").isin(rLowPriorityLine),
            addDays(col("GV_CREATED_ON"), lit(5))
          )
          .otherwise(
            when(
              col("BORD_TYPE") === lit("C") && 
              col("BACQADVCD").isin(rAac2Line) && 
              col("BADMLDTME") > lit(0) && 
              col("BPRODLDTM") > lit(0),
              date_format(
                date_add(
                  from_unixtime(
                    unix_timestamp(col("GV_CREATED_ON"), "yyyyMMdd"),
                    "yyyy-MM-dd"
                  ),
                  col("BADMLDTME") + col("BPRODLDTM")
                ),
                "yyyyMMdd"
              )
            )
            .otherwise(lit(""))
          )
        )
      )
    )
  }

  def calculateBtgtShip: Column = {
    when(col("BOFITMCAT").isin(matlItemCatGrpList1), lit(""))
      .otherwise(
        calculateBtgtShipForItemCategories
          .otherwise(
            calculateBtgtShipForDocType1
              .otherwise(
                calculateBtgtShipForDocType2
                  .otherwise(
                    when(col("DOC_TYPE").isin(rSalesDocType3Line), col("DSDEL_DATE"))
                      .otherwise(
                        when(
                          col("DOC_TYPE").isin(rSalesDocType4Line) && 
                          length(col("DSDEL_DATE")) > lit(0),
                          addDays(col("DSDEL_DATE"), lit(5))
                        )
                        .otherwise(
                          when(
                            col("DOC_TYPE").isin(rSalesDocType5Line) && 
                            col("ITEM_CATEG").isin(rSalesDocItemCat2Line) && 
                            length(col("GV_CREATED_ON")) > lit(0),
                            addDays(col("GV_CREATED_ON"), lit(197))
                          )
                          .otherwise(
                            calculateBtgtShipForFmsLogic
                              .otherwise(
                                calculateBtgtShipForSpecialCases
                              )
                          )
                        )
                      )
                  )
              )
          )
      )
  }
}

/************************************************************
 * bof20 to bof30 functions
 ************************************************************/

def dateParse2(rawDate: String): String = {
  try {
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val calendar = new GregorianCalendar()

    calendar.setLenient(false)
    calendar.set(
      Integer.parseInt(rawDate.substring(0, 4)),
      Integer.parseInt(rawDate.substring(4, 6)),
      Integer.parseInt(rawDate.substring(6, 8))
    )

    formatter.setLenient(false)
    formatter.format(calendar.getTime)
  } catch {
    case _: Throwable => null
  }
}

val dateParseUdf = udf[String, String](dateParse2)

def julianDateParse(myJulianYDDD: String): String = {
  try {
    val sysDate = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime)
    val sysDateDecade = sysDate.substring(0, 3)
    if (sysDateDecade + myJulianYDDD > sysDate)
      (Integer.parseInt(sysDateDecade) - 1) + myJulianYDDD
    else
      sysDateDecade + myJulianYDDD
  } catch {
    case _: Throwable => null
  }
}

val julianDateParseUDF = udf[String, String](julianDateParse)

def addDays = udf((rawDate: String, daysToAdd: Int) => {
  try {
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val calendar = new GregorianCalendar()

    calendar.setLenient(false)
    calendar.set(
      Integer.parseInt(rawDate.substring(0, 4)),
      Integer.parseInt(rawDate.substring(4, 6)) - 1,
      Integer.parseInt(rawDate.substring(6, 8))
    )
    calendar.add(Calendar.DATE, daysToAdd)

    formatter.format(calendar.getTime)
  } catch {
    case _: Throwable => null
  }
})

def lagDate: String => String = inDate => {
  try {
    val year = Integer.parseInt(inDate.substring(0, 4))
    val month = Integer.parseInt(inDate.substring(4, 6))
    val adjustedMonth = month + 2
    val finalMonth = 
      if (adjustedMonth > 12) f"${adjustedMonth - 12}%02d" 
      else f"$adjustedMonth%02d"
    val finalYear = if (adjustedMonth > 12) year + 1 else year

    s"$finalYear$finalMonth00"
  } catch {
    case _: Throwable => null
  }
}

val lagDateUDF = udf[String, String](lagDate)

def getLastDoM: String => String = rawDate => {
  try {
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val calendar = new GregorianCalendar()

    calendar.setLenient(false)
    calendar.set(
      Integer.parseInt(rawDate.substring(0, 4)),
      Integer.parseInt(rawDate.substring(4, 6)) - 1,
      Integer.parseInt(rawDate.substring(6, 8))
    )
    calendar.set(
      Calendar.DAY_OF_MONTH, 
      calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
    )

    formatter.format(calendar.getTime)
  } catch {
    case _: Throwable => null
  }
}

val getLastDoMUdf = udf[String, String](getLastDoM)

def selectActShip(
    mdDF: DataFrame,
    bofDF: DataFrame,
    byItemCatReasonRejActgiConfDateOrderByBcnfactsp: WindowSpec
): Try[DataFrame] = Try {
  val columns = bofDF.columns :+ "BCNFCHR01"

  mdDF
    .join(
      bofDF,
      when(mdDF("ITEM_CATEG") === "*", true)
        .otherwise(mdDF("ITEM_CATEG") === bofDF("ITEM_CATEG")) &&
        when(mdDF("BACTGIFLG") === "*", true)
        .otherwise(mdDF("BACTGIFLG") === bofDF("ACT_GI_FLAG")) &&
        when(mdDF("BREAS_REJ") === "*", true)
        .otherwise(mdDF("BREAS_REJ") === bofDF("REASON_REJ_CHK")) &&
        when(mdDF("BCONFDFLG") === "*", true)
        .otherwise(mdDF("BCONFDFLG") === bofDF("CONF_DATE_FLAG"))
    )
    .drop(mdDF("ITEM_CATEG"))
    .withColumn(
      "BCNFACTSP_RULE_RANK", 
      rank().over(byItemCatReasonRejActgiConfDateOrderByBcnfactsp)
    )
    .filter($"BCNFACTSP_RULE_RANK" === lit(1))
    .select(columns.map(expr): _*)
}

def selectShipQty(
    mdDF: DataFrame,
    bofDF: DataFrame,
    byBORDTYPEandITEMCATEGCONFDATEFLAGorderedByBCNFSHPQY: WindowSpec
): Try[DataFrame] = Try {
  val columns = bofDF.columns :+ "BCNFCHR01"

  bofDF
    .join(
      mdDF,
      when(mdDF("ITEM_CATEG") === "*", true)
        .otherwise(mdDF("ITEM_CATEG") === bofDF("ITEM_CATEG")) &&
        when(mdDF("BORD_TYPE") === "*", true)
        .otherwise(mdDF("BORD_TYPE") === bofDF("BORD_TYPE")) &&
        when(mdDF("BCONFDFLG") === "*", true)
        .otherwise(bofDF("CONF_DATE_FLAG") === mdDF("BCONFDFLG"))
    )
    .drop(mdDF("ITEM_CATEG"))
    .drop(mdDF("BORD_TYPE"))
    .withColumn(
      "BCNFSHPQY_RULE_RANK", 
      rank().over(byBORDTYPEandITEMCATEGCONFDATEFLAGorderedByBCNFSHPQY)
    )
    .filter($"BCNFSHPQY_RULE_RANK" === lit(1))
    .select(columns.map(expr): _*)
}

def selectBavailind(bofo20DF: DataFrame): Try[DataFrame] = Try {
  val reasonList1 = List("R2", "R3", "R5", "R7", "R8")
  val reasonList2 = List("R4", "R6", "AA", "AR", "IR")
  val reasonList3 = List("CB", "Z3", "Z5", "BN", "OA", "ZJ", "ZF", "ZS", "MB", "MC", "FI")
  val reasonList4 = List(
    "TAN ", "TANN", "ZBDF", "ZBDP", "ZBDQ", "ZBDU", "ZKIT", "ZPRS", 
    "ZRDD", "ZRFC", "ZRTN", "ZSA5", "ZSAF", "ZSDD", "ZSPP", "ZTAC"
  )
  val sysDate = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime)

  // Helper function for ZTVC/ZCAT document type logic
  def ztvcZcatLogic(reasonList1: List[String], reasonList2: List[String]): Column = {
    when(col("ITEM_CATEG") === "ZCAT", lit("Y"))
      .otherwise(
        when(col("REASON_REJ").isin(reasonList1: _*), lit("R"))
          .otherwise(
            when(col("CONF_QTY") === col("BTKILLQTY") && col("REASON_REJ") === "AK", lit("R"))
              .otherwise(
                when(col("BACT_SHIP") > col("BTGT_SHIP"), lit("N"))
                  .otherwise(
                    when(col("REASON_REJ").isin(reasonList2: _*), lit("N"))
                      .otherwise(
                        when(
                          length(col("BACT_SHIP")) > lit(0) && 
                          col("BACT_SHIP") <= col("BTGT_SHIP") && 
                          col("CONF_QTY") <= col("GR_QTY"), 
                          lit("Y")
                        )
                        .otherwise(
                          when(
                            col("ITEM_CATEG") === "ZTVR" && 
                            sysDate >= col("BTGT_SHIP") && 
                            length(col("BCREATEDT")) < 1, 
                            lit("N")
                          )
                          .otherwise(
                            when(
                              length(col("BACT_SHIP")) < 1 && 
                              sysDate <= col("LAG_DATE"), 
                              lit("L")
                            )
                            .otherwise(
                              when(
                                length(col("BACT_SHIP")) > lit(0) && 
                                col("BACT_SHIP") <= col("BTGT_SHIP") && 
                                sysDate <= col("LAG_DATE") && 
                                col("CONF_QTY") > col("GR_QTY"), 
                                lit("L")
                              )
                              .otherwise(lit("N"))
                            )
                          )
                        )
                      )
                  )
              )
          )
      )
  }

  // Helper function for other document types logic  
  def otherDocTypesLogic(reasonList3: List[String], reasonList4: List[String]): Column = {
    when(col("REASON_REJ") === "CB", lit("N"))
      .otherwise(
        when(length(col("BTGT_SHIP")) < lit(1), lit(" "))
          .otherwise(
            when(col("ITEM_CATEG") === "ZTXT", lit(" "))
              .otherwise(
                when(
                  col("ITEM_CATEG") === "ZSTO" && 
                  col("GR_QTY") <= lit(0) && 
                  length(col("DOC_DATE")) < lit(1) && 
                  sysDate > col("LAG_DATE"), 
                  lit("R")
                )
                .otherwise(
                  when(
                    col("REASON_REJ") > lit(" ") && 
                    (sysDate <= col("LAG_DATE") || col("REASON_REJ").isin(reasonList3: _*)), 
                    lit("R")
                  )
                  .otherwise(
                    when(
                      length(col("BACT_SHIP")) > lit(0) && 
                      col("BACT_SHIP") <= col("BTGT_SHIP") && 
                      (col("CONF_QTY") <= col("ZQUANTITY") || 
                       (col("LOWR_BND") > lit(0) && col("REASON_REJ").isin(reasonList4: _*))), 
                      lit("Y")
                    )
                    .otherwise(
                      when(col("BACT_SHIP") > col("BTGT_SHIP"), lit("N"))
                        .otherwise(
                          when(
                            sysDate >= col("BTGT_SHIP") && 
                            length(col("DOC_DATE")) < lit(1) && 
                            length(col("BCREATEDT")) < lit(1), 
                            lit("N")
                          )
                          .otherwise(
                            when(
                              length(col("BACT_SHIP")) < lit(1) && 
                              sysDate <= col("LAG_DATE"), 
                              lit("L")
                            )
                            .otherwise(
                              when(
                                length(col("BACT_SHIP")) > lit(0) && 
                                col("BACT_SHIP") <= col("BTGT_SHIP") && 
                                sysDate <= col("LAG_DATE") && 
                                col("CONF_QTY") > col("GR_QTY"), 
                                lit("L")
                              )
                              .otherwise(lit("N"))
                            )
                          )
                        )
                    )
                  )
                )
              )
          )
      )
  }

  bofo20DF
    .withColumn("LAG_DATE", lagDateUDF(col("BTGT_SHIP")))
    .withColumn("ZQUANTITY", when(col("GR_QTY") > lit(0), col("GR_QTY")).otherwise(col("BSHIP_QTY")))
    .withColumn(
      "BAVAILIND",
      when(col("DOC_TYPE").isin("ZTVC", "ZCAT"), ztvcZcatLogic(reasonList1, reasonList2))
        .otherwise(otherDocTypesLogic(reasonList3, reasonList4))
    )
    .select((bofo20DF.columns :+ "BAVAILIND").map(col): _*)
}

// BOFO15
val bofo15Keys = "DOC_NUMBER,S_ORD_ITEM"
val bofo15Fields = 
  "DOC_NUMBER,S_ORD_ITEM,SCHED_LINE,DELIV_NUMB,DELIV_ITEM,REQU_QTY,CONF_QTY,DLV_QTY,SALES_UNIT," +
  "DLV_STSO,DLV_STS,GI_STS,LW_GISTS,DSDEL_DATE,DSDEL_TIME,CONF_DATE,CONF_TIME,PLD_GI_DTE," +
  "ACT_GI_DTE,LST_A_GD,ACT_DL_DTE,ACT_DL_TME,LST_A_DD,LST_A_DT,DLV_BLOCKD,BASE_UOM,DENOMINTR," +
  "NUMERATOR,DOC_CATEG,SCHED_DEL,REC_DELETD,BMATL_BLK,DLVQEYCR,DLVQEYSC,DLVQLECR,DLVQLESC," +
  "BCOUNTER,BCNDQTY,BEARCDTE"
val bofo15UsedForBofo30Seq = 
  "DOC_CATEG,DOC_NUMBER,S_ORD_ITEM,SALES_UNIT,DLV_STSO,DLV_STS,LW_GISTS,DSDEL_DATE,LST_A_GD,BMATL_BLK,LST_A_GD,BMATL_BLK"
    .split(",").toSeq
val bofo15Schema = StructType(
  bofo15Fields.split(",").toList.map(x => schemaMapper(x, bofo15Keys.split(",").toList))
)

// BOFO12
val bofo12Keys = "DOC_NUMBER,S_ORD_ITEM"
val bofo12Fields = "DOC_NUMBER,S_ORD_ITEM,STS_ITM,STS_BILL,STS_PRC,STS_DEL,DOC_TYPE,COMP_CODE,LOC_CURRCY,SOLD_TO,CUST_GRP1,CUST_GRP2,CUST_GRP3,CUST_GRP4,CUST_GRP5,DEL_BLOCK,DOC_CATEG,SALES_OFF,SALES_GRP,SALESORG,DISTR_CHAN,REASON_REJ,CH_ON,ORDER_PROB,GROSS_WGT,BWAPPLNM,PROCESSKEY,BATCH,EXCHG_CRD,EANUPC,CREATEDON,CREATEDBY,CREA_TIME,BILBLK_ITM,UNIT_OF_WT,CML_CF_QTY,CML_CD_QTY,COND_UNIT,SALESDEAL,COND_PR_UN,CML_OR_QTY,SUBTOTAL_1,SUBTOTAL_2,SUBTOTAL_3,SUBTOTAL_4,SUBTOTAL_5,SUBTOTAL_6,MIN_DL_QTY,STOR_LOC,REQDEL_QTY,MATL_GROUP,MATERIAL,MAT_ENTRD,BASE_UOM,MATL_GRP_1,MATL_GRP_2,MATL_GRP_3,MATL_GRP_4,MATL_GRP_5,TAX_VALUE,NET_PRICE,NET_VALUE,NET_WT_AP,UNLD_PT_WE,BILLTOPRTY,PAYER,SHIP_TO,PROD_HIER,FORWAGENT,ITEM_CATEG,SALESEMPLY,ROUTE,SHIP_STCK,DIVISION,STAT_DATE,EXCHG_STAT,SUB_REASON,BND_IND,UPPR_BND,DENOMINTR,NUMERATOR,DENOMINTRZ,NUMERATORZ,LOWR_BND,ST_UP_DTE,REFER_DOC,REFER_ITM,PRVDOC_CTG,VOLUMEUNIT,VOLUME_AP,SALES_UNIT,SHIP_POINT,DOC_CURRCY,COST,PLANT,TARGET_QU,TARGET_QTY,TARG_VALUE,BADV_CD,BBILL_AD,BILL_TYPE,BBO_TYPE,BBILL_ST,BCAGE_CD,BCNLDDT,B_CAS,BCOMMODTY,BCONDITN,BCONT_LN,BCNTRLNBR,B_CRM,BDISTRIB,BDMDPLANT,BDOCIDCD,BDODAAC,BTGT_SHIP,BEXC_INFO,B_SST,BFMS_PRG,BFUND_CD,BINVCNT,BLOA,BCLSSA,BPO_NUM,BPO_ITMNO,BMEDST_CD,BMGMT_CD,BMIPR,BMIPRNBR,BMODDT,BNS_RDD,BORGDNUM,BOWNRSHP,BPRIUSRID,BPRI_CD,BPROJ_CD,BPONBR,BPURPRIC,BPURPOSE,BRCD_NUM,BCONDCTRL,BRETNTQTY,BRICFROM,BSTDDLVDT,BSUFX_CD,BSHIPNBR,BSIG_CD,BSPLPGMRQ,BSUPP_ST,BSUPPADR,SALES_DIST,SERV_DATE,BILL_DATE,INCOTERMS,INCOTERMS2,CUST_GROUP,ACCNT_ASGN,EXCHG_RATE,TRANS_DATE,PRICE_DATE,RT_PROMO,PRODCAT,ORDER_CURR,DIV_HEAD,DOC_CATEGR,WBS_ELEMT,ORD_ITEMS,FISCVARNT,PROFIT_CTR,CO_AREA,BCONVERT,BCUSTPODT,BBSTNK,REJECTN_ST,QUOT_FROM,ORD_REASON,QUOT_TO,BILL_BLOCK,BAUDAT,RATE_TYPE,STAT_CURR,BKDMAT,BMFRPRTNO,BPROJ_NUM,BMFR_CAGE,BMFR_NAME,DOCTYPE,BITMCATGP,DSDEL_DATE,BDISCP_CD,BMATRCVDT,BMRA_QTY,BDISCPQTY,BORDER_DT,BOFBNSRDD,BOFITMCAT,BOFPRICD,BZZESD,BRIC_TO,BMOQ,BHDRCRTDT,BZZSSC,BZZDMDDN,BZZDMDSC,BZZODNPST,BZZJOKO,BZZUSECD,BZZCHRNCD,BZZPRIREF,BZZINSPCD,BZZPICKLS,BZZMATAQC,BZZRICSOS,BZZUTILCD,BZREPMATL,BUEPOS,BSTLNR,BZZSRC_CD,BEARCDTE,BRB_DTID,BFREXP_DT,BZZCUR_DT,BFLPICKUP,BCONBIDPR,BTRQTYSUN,BCONISFFL,DLV_PRIO,BSHRTTXT,BGFN_CLIN,BGFM_CONT,BGFM_CALL,BGFM_MDN,BPS_POPST,BPS_POPEN,BMIPR_NBR,BTPDDATE,BTPDQTY"
val bofo12UsedForBofo30Seq = "DOC_NUMBER,S_ORD_ITEM,STS_ITM,STS_BILL,STS_PRC,STS_DEL,DOC_TYPE,COMP_CODE,SOLD_TO,OBPARTNER,CUST_GRP1,CUST_GRP2,CUST_GRP3,CUST_GRP4,CUST_GRP5,DEL_BLOCK,DOC_CATEG,SALES_OFF,SALES_GRP,SALESORG,DISTR_CHAN,REASON_REJ,CH_ON,BATCH,BSHIP_MON,CALMONTH,CREATEDON,CREATEDBY,CREA_TIME,STOR_LOC,MATL_GROUP,MATERIAL,MATL_GRP_1,MATL_GRP_2,MATL_GRP_3,MATL_GRP_4,MATL_GRP_5,BILLTOPRTY,PAYER,SHIP_TO,PROD_HIER,FORWAGENT,ITEM_CATEG,ROUTE,SHIP_STCK,DIVISION,STAT_DATE,BND_IND,ST_UP_DTE,REFER_DOC,REFER_ITM,SALES_UNIT,SHIP_POINT,PLANT,BADV_CD,BBILL_AD,BILL_TYPE,BBO_TYPE,BBILL_ST,BCAGE_CD,BCNLDDT,B_CAS,BCONDITN,BCONT_LN,BCNTRLNBR,B_CRM,BDISTRIB,BDMDPLANT,BDOCIDCD,BDODAAC,BEXC_INFO,B_SST,BFMS_PRG,BFUND_CD,BINVCNT,BLOA,BCLSSA,BMEDST_CD,BMGMT_CD,BMIPR,BMIPRNBR,BMODDT,BORGDNUM,BOWNRSHP,BPRIUSRID,BPROJ_CD,BPONBR,BPURPOSE,BCONDCTRL,BRETNTQTY,BRICFROM,BSUFX_CD,BSHIPNBR,BSIG_CD,BSPLPGMRQ,BSUPP_ST,BSUPPADR,BILL_DATE,CUST_GROUP,PRICE_DATE,DOC_CATEGR,WBS_ELEMT,FISCVARNT,PROFIT_CTR,CO_AREA,REJECTN_ST,BAUDAT,BKDMAT,BMFRPRTNO,BPROJ_NUM,BMFR_CAGE,BMFR_NAME,DOCTYPE,DSDEL_DATE,BDISCP_CD,BMATRCVDT,BMRA_QTY,BDISCPQTY,BRIC_TO,BMOQ,BHDRCRTDT,BZZSSC,BZZDMDDN,BZZDMDSC,BZZODNPST,BZZJOKO,BZZUSECD,BZZCHRNCD,BZZPRIREF,BZZINSPCD,BZZPICKLS,BZZMATAQC,BZZRICSOS,BZZUTILCD,BGFN_CLIN,BGFM_CONT,BGFM_CALL,BMIPR_NBR,BGFM_MDN,BTPDDATE,BTPDQTY,BSTAGRP".split(",").toSeq
val bofo12Schema = StructType(
  bofo12Fields.split(",").toList.map(x => schemaMapper(x, bofo12Keys.split(",").toList))
)

// BOFO20
val bofo20Keys = "DOC_NUMBER,S_ORD_ITEM"
val bofo20Fields = "DOC_NUMBER,S_ORD_ITEM,BFMS_PRG,BFMSCSCD,BFMS_IND,BMILSVC_1,BMILSVC_2,BMILSVC_G,BMILSVC_S,BORDER_DT,MIN_DL_QTY,BASE_UOM,LOWR_BND,DOC_CURRCY,BPURPRIC,RTPLCST,CML_CF_QTY,BTORDQTY,BTKILLQTY,BTKILLUOM,BTVRSTCD,CREATEDON,CONF_QTY,NET_VALUE,REQ_QTY,BCNDQTY,BDMDQTY,GIS_QTY,BCONVERT,MATERIAL,BIPG,SUBTOTAL_3,SUBTOTAL_4,SUBTOTAL_5,SUBTOTAL_6,SUBTOTAL_2,SUBTOTAL_1,QCOASREQ,LOAD_DATE,SOLD_TO,DLV_QTY,REQDEL_QTY,BGR_DATE,RECORDMODE,BCUSTPODT,BNS_RDD,BORD_TYPE,BBSTNK,BORGDNUM,BORGNLDOC,BPRI_CD,CML_OR_QTY,DSDEL_DATE,BMPC,ITEM_CATEG,REASON_REJ,LOC_CURRCY,UPPR_BND,SALES_UNIT,GR_QTY,BSHIPTYP,BITMCATGP,BTSALEUNT,COND_PR_UN,NET_PRICE,BPONBR,ACT_GI_DTE,DOC_TYPE,PO_UNIT,UNIT,PCONF_QTY,CML_CD_QTY,CONF_DATE,BCAGECDPN,BIDNLF,BMFRPN,DLVQEYCR,DLVQEYSC,DLVQLECR,BMATRCVDT,BNMCS_IND,BCASREP,DLVQLESC,BCREATEDT,DOC_DATE,B_YOBLOCK,B_ZTBLOCK,BOFBNSRDD,BOFITMCAT,BOFPRICD,BPLT,BACQADVCD,SCL_DELDAT,B_ALT,DLV_BLOCK,BMRA_QTY,CUST_GRP3,BZZESD,BEARCDTE"
val bofo20UsedForBofo30Seq = "DOC_NUMBER,S_ORD_ITEM,DOC_TYPE,PSTNG_DATE,BORDER_DT,CML_CD_QTY,MIN_DL_QTY,BASE_UOM,LOWR_BND,DOC_CURRCY,BPURPRIC,CONF_QTY,NET_VALUE,GIS_QTY,DLVQEYCR,DLVQEYSC,DLVQLECR,BCONVERT,DLVQLESC,SUBTOTAL_3,SUBTOTAL_4,SUBTOTAL_5,SUBTOTAL_6,SUBTOTAL_2,SUBTOTAL_1,QCOASREQ,DLV_QTY,BCUSTPODT,BNS_RDD,BORD_TYPE,BBSTNK,BORGDNUM,BPRI_CD,BMPC,LOC_CURRCY,UPPR_BND,SALES_UNIT,BSHIP_QTY,BTGT_SHIP,BTGTSHIP,BZZESD,BFMS_PRG,BITMCATGP,CML_CF_QTY,REQDEL_QTY,BFMSCSCD,BIPG,DSDEL_DATE,GR_QTY,REQ_QTY,REASON_REJ,SOLD_TO,BTKILLQTY,BTKILLUOM,BTORDQTY,BTVRSTCD,BSHIPTYP,BMRA_QTY,CML_OR_QTY,CREATEDON,ITEM_CATEG,MATERIAL,NET_PRICE,LOAD_DATE,COND_PR_UN,BACT_SHIP,BAVAILIND,REQU_QTY,ACT_GI_DTE,PO_UNIT,PCONF_QTY,BNMCS_IND,BCASREP,CONF_DATE,BACTSHIP,BCAGECDPN,BIDNLF,BCREATEDT,DOC_DATE,B_YOBLOCK,B_ZTBLOCK,BMFRPN,BOFBNSRDD,BOFITMCAT,BPLT,SCL_DELDAT,B_ALT,BCNDQTY,BOFPRICD".split(",").toSeq
val bofo20Schema = StructType(
  bofo20Fields.split(",").toList.map(x => schemaMapper(x, bofo20Keys.split(",").toList))
)
// BOFO08
val bofo08Keys = "DOC_NUMBER,S_ORD_ITEM"
val bofo08Fields = "DOC_NUMBER,S_ORD_ITEM,RECORDMODE,BTIMESTMP,BCSTACCDT,BCSTACCQY,BEMRAIND,BEMRADTE,DOC_TYPE,DOC_CATEG,ITEM_CATEG,SALES_UNIT,BQMNUM,BEMRAQTY,BTAC_FMS,BTACFMSIN,BTAC_OOT,BTACOOTIN,BTACFDT,BTACFDTI,BTRNTYPCD,BBASISACK,BSHPMANUM,BSDAQUAN,BSDADATE,BPIIN_13,BEARCDTE"
val bofo08UsedForBofo30Seq = "DOC_NUMBER,S_ORD_ITEM,BCSTACCDT,BCSTACCQY,BEMRADTE,BEMRAQTY,BTRNTYPCD,BBASISACK,BSHPMANUM,BSDAQUAN,BSDADATE".split(",").toSeq
val bofo08Schema = StructType(
  bofo08Fields.split(",").toList.map(x => schemaMapper(x, bofo08Keys.split(",").toList))
)

// BCNFMILS
val BCNFMILSPFields = "BCNFMILSP,OBJVERS,CHANGED,BSERVICE1,BSERVICE2,BSERVICE3,BFMS_IND,BMILGP1FG,BMILGP2FG,BMILGP3FG,BMILGP1RS,BMILGP2RS,BMILGP3RS"
val BCNFMILSPUsedForBofo30Seq = ""
val BCNFMILSPSchema = StructType(
  BCNFMILSPFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// ZPCUSTOMER
val ZPCUSTOMERFields = "CUSTOMER,OBJVERS,CUSTGRP3"
val ZPCUSTOMERUsedForBofo30Seq = ""
val ZPCUSTOMERSchema = StructType(
  ZPCUSTOMERFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// BCNFSTAGP
val BCNFSTAGPFields = "BCNFSTAGP,OBJVERS,CHANGED,BCNFCHR01,DOC_TYPE"
val BCNFSTAGPUsedForBofo30Seq = "BPIIN_13,DOC_TYPE"
val BCNFSTAGPSchema = StructType(
  BCNFSTAGPFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// BCNFGIPG
val BCNFGIPGFields = "BCNFGIPG,OBJVER,CHANGED,BPRICODE,BCNFCHR01"
val BCNFGIPGUsedForBofo30Seq = ""
val BCNFGIPGSchema = StructType(
  BCNFGIPGFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// BCNFCASRP
val BCNFCASRPFields = "BCNFCASRP,OBJVERS,CHANGED,BORGDNO01,BORGDNO11,BCNFIPG,BCNFCHR01"
val BCNFCASRPUsedForBofo30Seq = ""
val BCNFCASRPSchema = StructType(
  BCNFCASRPFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// BCNFNMCS
val BCNFNMCSFields = "BCNFNMCS,OBJVERS,CHANGED,BCNFIPG,BNS_RDD,BCNFCHR01"
val BCNFNMCSUsedForBofo30Seq = ""
val BCNFNMCSSchema = StructType(
  BCNFNMCSFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// BCNFACTSP
val BCNFACTSPFields = "BCNFACTSP,OBJVERS,CHANGED,ITEM_CATEG,BACTGIFLG,BREAS_REJ,BCONFDFLG,BCNFCHR01"
val BCNFACTSPUsedForBofo30Seq = ""
val BCNFACTSPSchema = StructType(
  BCNFACTSPFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// BCNFSHPQY
val BCNFSHPQYFields = "BCNFSHPQY,OBJVERS,CHANGED,BORD_TYPE,ITEM_CATEG,BCONFDFLG,BCNFCHR01"
val BCNFSHPQYUsedForBofo30Seq = ""
val BCNFSHPQYSchema = StructType(
  BCNFSHPQYFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// BICPPCTR
val BICPPCTRFields = "BICPPCTR,OBJVERS,CHANGED,BLOPRCTR,BHIPRCTR,BMM_PLT"
val BICPPCTRUsedForBofo30Seq = ""
val BICPPCTRSchema = StructType(
  BICPPCTRFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// Material
val MaterialFields = "MATERIAL,OBJVERS,CHANGED,AF_COLOR,AF_FCOCO,AF_GENDER,AF_GRID,AF_STYLE,APO_PROD,BASE_UOM,BASIC_MATL,BBP_PROD,COMPETITOR,CONT_UNIT,CREATEDON,CRM_PROD,DIVISION,EANUPC,EXTMATLGRP,GROSS_CONT,GROSS_WT,HC_AGENT1,HC_AGENT2,HC_AGENT3,HC_ANESIND,HC_APPRTYP,HC_ATCCODE,HC_ATCMTYP,HC_CATIND1,HC_CATIND2,HC_CATIND3,HC_HAZMIND,HC_IMPMIND,IND_SECTOR,LOGSYS,MANUFACTOR,MANU_MATNR,MATL_CAT,MATL_GROUP,MATL_TYPE,MSA_USAGE,NET_CONT,NET_WEIGHT,PO_UNIT,PROD_HIER,RT_CONFMAT,RT_PRBAND,RT_PRRULE,RT_SEASON,RT_SEAYR,RT_SUPS,SIZE_DIM,STD_DESCR,UCCERTIFTY,UCCONSTCLA,UCFUNCCLAS,UNIT_DIM,UNIT_OF_WT,VOLUME,VOLUMEUNIT,BACQMTDCD,BACQMTDSX,BACQADVCD,BRTNMTDCD,BITMCNSRT,BSVCUCARM,BITMSTDCD,BITMNAMCD,BPRCMTLCD,BDEMILCD,BTECHCONT,BQUALCONT,BPACKCONT,BDEMNDPLR,BSUPLYPLR,BADPCODE,BENVATRCD,BHZDMATCD,BABCCLASS,BREPCHIND,BCIIC,BSVCUCNAV,BSVCUCAF,BSVCUCMAR,BSVCUCOTH,BPKGREVCD,BTECHRVCD,BSPLITMCD,BICC,BSRGINDCD,BSMRECVY,BPRIORRTE,BMNDSRCCD,BSTDSICCD,BITMCLSCD,BCTLACTCD,BSUBSID,BMATLSFST,BPROCGRCD,BADMLDTME,BTECHRVDT,BBAGITMID,BREPLNSN,BSOLSRCCD,BSOLSRCDT,BRATPGMID,BCOVDURTN,BPRODLDTM,BMTDCMPCD,BPROJCODE,BWEIFLIS,BSUBITMCD,BVOLFLIS,BMATLLOCK,BRELCONFM,B_PGC,BMGTGRPCD,CURRENCY,PRICE_STD,BUNITCUBE,MATL_GRP_4,B_NSN,B_NIIN,BPRCGRCD5,BSPCCODE,BMFCSTBSW,BMCLABSW,BMLEADTIM,BMZZWSIC,BMANUCOST,BMFSC,BMANU_UOM,BMATLDESC,BMDOMNTSW,BMANUEXDT,BMCONFCST,BMDISTCHN,BMANUPRCE,BMCONHIST,BMZZWSDC,BZZWSDC,BZZWSIC,BZZWSEC,BZZWSGC,CO_AREA,PROFIT_CTR,BMTLPRGRP,BFCST_IND,BIPT_CC,BPRCGRCD2,BFLIGHTSF,BLIFESUPT,BPLACEINS,BQLTYCTRL,BZZANAFLG,BZZLTCSTA,BZZSMSDRV,BZZSMSFLG,BLOGLOSSD,BMSUPCLAB,BMGFM,BMSHLFTYP,BADMDFRQ,BADMDQTY,BOWRMR,BITMCATGP,BREPMTDCD,DEL_FLAG,BOLD_MATL,BCRIT_IND,PRICE_AVG,BPR00_PRC,HEIGHT,LENGHT,LOC_CURRCY,RPA_WGH1,RPA_WGH2,RPA_WGH3,RPA_WGH4,RTPLCST,RT_COLOR,RT_FASHGRD,RT_MDRELST,RT_SEAROLL,RT_SEASYR,RT_SIZE,VENDOR,WIDTH,UNIT,BCONTRREQ,BQMPROCAT,BCRSDSTST,BCRSPLTST,BLTCAWDST,BLTCPROJS,BPRIORFYA,B2PRIORFY,BBASEPIIN,BBWPDATE,BSRC_PCMT,B_LTCAWDS,B_LTCPROJ,B_PRIORFY,B_2PRIORF,BAWD_SCHN,BAWD_SITE,BDCS_MATS,BDCS_MSVD,BDELG_PLT,BEX_STREV,BIND_COLL,BMDS,BMDS_EXDT,BMDS_LCD,BPROC_TYP,BSPE_PRTY,BXDCMS_VD,BXPMS_VDT,DOC_TYPE,DOC_NUM,BDOC_VERS,BMUNITCST,BMBDC,PRICE_MAT,SALES_UNIT,BZBPR_PRC,B_SALK3,B_LBKUM,BKEYCODE,BPRICEUNT,BKZEFF,BPEINH,BSOLSRCDA,BREPPRICE,BCUMMINSS,BCUMMAXOH,BAGPROTLV,BIADOCCAG,BIADOCNBR,BIAPCENBR,BIAREVNBR,BIACNVFAC,BZUMNFLAG,BCOG,B_SMIC,BMCC,BSMCC,BIASLACD,BIAMANAAC,BACTIVSKU,BAFERRCCD,BMCRCCODE,BARCCODE,BRETITMSW,BIAUOM,BKITENDIT,BKITCMPIN,B_CHG_ON,BZZ_LOB,BNRGMATTP,BLUST_TAX,BGAINLOSS,BCHEMCON,BQASCON,BMATWRT,BEXCLRSN,BNRECPRM"
val MaterialUsedForBofo30Seq = "BACQADVCD,BADMLDTME,BPRODLDTM,PROFIT_CTR,OBJVERS,MATERIAL"
val MaterialSchema = StructType(
  MaterialFields.split(",").toList.map(x => schemaMapper(x, "".split(",").toList))
)

// bofo30
val bofo30Keys = "DOC_NUMBER,S_ORD_ITEM"
val bofo30Fields =
  """DOC_NUMBER,S_ORD_ITEM,DOC_TYPW,BPONBR,BMIPRNBR,BMGMT_CD,BMEDST_CD,BINVCNT,PSTNG_DATE,CURRENCY,
    |BORDER_DT,CML_CD_QTY,MIN_DL_QTY,BASE_UOM,LOWR_BND,DOC_CURRCY,BPURPRIC,CONF_QTY,NET_VALUE,GIS_QTY,
    |DLVQEYCR,DLVQEYSC,DLVQLECR,BCONVERT,DLVQLESC,SUBTOTAL_3,SUBTOTAL_4,SUBTOTAL_5,SUBTOTAL_6,
    |SUBTOTAL_2,SUBTOTAL_1,QCOASREQ,DLV_QTY,BGR_DATE,BCUSTPODT,BNS_RDD,BORD_TYPE,BBSTNK,BORGDNUM,
    |BPRI_CD,BMPC,LOC_CURRCY,UPPR_BND,SALES_UNIT,BSHIP_QTY,BTGT_SHIP,BTGTSHIP,BFUND_CD,BLOA,BMIPR,
    |BMODDT,BPROJ_CD,BPURPOSE,BTYPEASST,MATL_GRP_1,MATL_GRP_2,MATL_GRP_3,MATL_GRP_5,PAYER,PLANT,
    |PRICE_DATE,PROD_HIER,PROFIT_CTR,REFER_DOC,REFER_ITM,REJECTN_ST,ROUTE,SALES_GRP,SALES_OFF,
    |SHIP_POINT,SHIP_STCK,SHIP_TO,STAT_DATE,STOR_LOC,STS_BILL,STS_ITM,STS_PRC,STS_DEL,WBS_ELEMT,
    |BADV_CD,BBILL_AD,BBIL_ST,BCAGE_CD,BCLSSA,BCNLDDT,BCNTRLNBR,BCONDCTRL,BCONT_LN,BDEMAND,BDISTRIB,
    |BDOCIDCD,BDODAAC,BEXC_INFO,BFMS_PRG,BPRIUSRID,BRETNTQTY,BRICFROM,BSHIPNBR,BSIG_CD,BSPLPGMRQ,
    |BSUPPADR,SALESORG,BOWNRSHP,BZZSED,MATL_GRP4,BMFR_NAME,BMFRPRTNO,BKDMAT,BPROJ_NUM,STORNO,BCOUNTER,
    |BMFR_CAGE,BITMCATGP,BMATRCVDT,BSUFX_CD,BSUPP_ST,DLV_STSO,DLV_STS,LST_A_GD,LW_GISTS,BMATL_BLK,
    |CML_CF_QTY,REQDEL_QTY,BFMSCSCD,BIPG,DSDEL_DATE,GR_QTY,REQ_QTY,REASON_REJ,SOLD_TO,BTKILLQTY,
    |BTKILLUOM,BTORDQTY,BTVRSTCD,BBO_TYPE,BSHIPTYP,BDISCP_CD,BMRA_QTY,BDISCPQTY,BATCH,BILL_DATE,
    |BILL_TYPE,BILLTOPRTY,BND_IND,CH_ON,CML_OR_QTY,CO_AREA,COMP_CODE,CREA_TIME,CREATEDBY,CREATEDON,
    |CUST_GROUP,CUST_GRP1,CUST_GRP2,CUST_GRP3,CUST_GRP4,CUST_GRP5,DEL_BLOCK,DISTR_CHAN,DIVISION,
    |DOC_CATEG,DOC_CATEGR,FISCVARNT,FORWAGENT,ITEM_CATEG,MATERIAL,MATL_GROUP,ST_UP_DTE,B_CAS,B_CRM,
    |B_SST,BCONDITN,BDMDPLANT,NET_PRICE,LOAD_DATE,BSTAGRP,COND_PR_UN,BACT_SHIP,BCNDQTY,BDMDQTY,
    |BAVAILIND,CALMONTH,ITEM_DEL,REQU_QTY,BZZRIC,BSHIP_MON,BPIIN_13,ACT_GI_DTE,PO_UNIT,BMILSVC_1,
    |BMILSVC_2,BMILSVC_3,BAUDAT,PCONF_QTY,BNMCS_IND,BCASREP,CONF_DATE,DOCTYPE,BACTSHIP,BCAGECDPN,
    |BIDNLF,BCREATEDT,DOC_DATE,B_YOBLOCK,B_ZTBLOCK,BMFRPN,BOFBNSRDD,BOFITMCAT,BPLT,SCL_DELDAT,
    |B_ALT,BOFPRICD,BMOQ,BRIC_TO,BHDRCRTDT,CONF_TYPE,BPARTNER,BZZSSC,BZZDMDDN,BZZDMDSC,BZZODNPST,
    |BZZJOKO,BZZUSECD,BZZCHRNCD,BZZPRIREF,BZZINSPCD,BZZPICKLS,BZZMATAQC,BZZRICSOS,BZZUTILCD,
    |BEARCDTE,BGFM_CLIN,BGFM_CONT,BGFM_CALL,BMIPR_NBR,BGFM_MDN,BEMRADTE,BCSTACCDT,BCSTACCQY,BTPDDATE,
    |BTPDQTY,BEMRAQTY,BTRNTYPCD,BBASISACK,BSHPMANUM,BSDAQUAN,BSDADATE""".stripMargin

val bofo30Schema = StructType(
  bofo30Fields.split(",").toList.map(x => schemaMapper(x, bofo30Keys.split(",").toList))
)

// Function to create test DataFrame
def createTestDataFrame(numOfRows: Int, schema: StructType): DataFrame = {
  spark.createDataFrame(
    (for (_ <- 1 to numOfRows) yield
      Row.fromSeq((for (a <- schema) yield a match {
        case f if f.dataType == DateType =>
          new java.sql.Date(
            new SimpleDateFormat("dd-MM-yyyy")
              .parse(LocalDate.ofEpochDay(new Random().nextInt(150000)).toString)
              .getTime()
          )
        case f if f.dataType == StringType && f.name == "DOC_CATEG" =>
          "ABCDEIK".charAt(scala.util.Random.nextInt(6)).toString
        case f if f.dataType == StringType && f.name == "OBJVERS" =>
          "ABCD".charAt(scala.util.Random.nextInt(4)).toString
        case f if f.dataType == StringType => scala.util.Random.alphanumeric.take(10).mkString
        case _                             => BigDecimal(scala.util.Random.nextInt(1000))
      }).toSeq))
    ).toList.asJava,
    schema
  )
}

// Create DataFrames
val rows = 15
val bof30DF = createTestDataFrame(rows, bofo30Schema)
val bof20DF = createTestDataFrame(rows, bofo20Schema)
val bof12DF = createTestDataFrame(rows, bofo12Schema)
val bof15DF = createTestDataFrame(rows, bofo15Schema)
val bof08DF = createTestDataFrame(rows, bofo08Schema)

// Additional DataFrames
val BCNFMILSPDF = Seq(
  ("0010", "A", "A", "A", "B", "A", "*", "Y", "A", "A", "AAD", "DDA", "AAAAAAAAAA"),
  ("0020", "B", "*", "*", "*", "*", "*", "Y", "A", "B", "DAA", "ACC", "BBBBBBBBBB"),
  ("0050", "A", "*", "B", "*", "A", "*", "Y", "A", "D", "ADA", "CCA", "CCCCCCCCCC")
).toDF(
  "BCNFMILSP",
  "OBJVERS",
  "CHANGED",
  "BSERVICE1",
  "BSERVICE2",
  "BSERVICE3",
  "BFMS_IND",
  "BMILGP1FG",
  "BMILGP2FG",
  "BMILGP3FG",
  "BMILGP1RS",
  "BMILGP2RS",
  "BMILGP3RS"
)

val ZPCUSTOMERDF = Seq(
  ("EJnb9IccRI", "A", "AAA"),
  ("0et2E5sTBM", "B", "ABB"),
  ("wTxdgxE1EW", "A", "ABB")
).toDF("CUSTOMER", "OBJVERS", "CUSTGRP3")

val BCNFACTSP = spark.read
  .format("csv")
  .option("header", "false")
  .option("delimiter", ",")
  .schema(BCNFACTSPSchema)
  .load("/data/bcnfactsp/BCNFACTSP.csv")

val BCNFSHPQY = spark.read
  .format("csv")
  .option("header", "false")
  .option("delimiter", ",")
  .schema(BCNFSHPQYSchema)
  .load("/data/bcnfshpqy/BCNFSHPQY.csv")

// Derived DataFrames
val bicppctrDF = BICPPCTR.filter($"BICPPCTR" > 0 && $"OBJVERS" === "A")
val BCNFACTSPDF = BCNFACTSP.filter(length($"BCNFACTSP") > 0)
val BCNFSHPQYDF = BCNFSHPQY.filter(length($"BCNFSHPQY") > 0)
val MaterialObjectVerADF = MaterialDF.filter($"OBJVERS" === "A")
val DistinctMaterialDF = MaterialDF
  .filter($"OBJVERS" === "A")
  .join(bof20DF, "MATERIAL")
  .select($"BACQADVCD", $"BADMLDTME", $"BPRODLDTM", $"PROFIT_CTR", $"OBJVERS", $"MATERIAL")
  .dropDuplicates


// Start Routine

val matlItemCatGrpList1: List[String] = List("ZMRE", "ZTXT")
val rSalesDocItemCat2Line: List[String] = List("ZCAP")
val rSalesDocItemCat1Line: List[String] = List("ZHEP")
val rSalesDocType1Line: List[String] = List("ZTVC", "ZIPV", "ZPPA", "ZAMA", "ZPPN", "ZRTO")
val rSalesDocType2Line: List[String] = List("ZTVR")
val rSalesDocType3Line: List[String] = List("ZFFV", "ZKIT")
val rSalesDocType4Line: List[String] = List("ZSTO")
val rSalesDocType5Line: List[String] = List("ZPNM")
val rFmsProgCdLine: List[String] = List("D", "E")
val rExpNsRdd1Line: List[String] = List("444", "555", "777", "999")
val rExpNsRdd2Line: List[String] = List("E", "N")
val rAac1Line: List[String] = List("H")
val rAac2Line: List[String] = List("F", "J", "L", "N", "O", "T", "U", "W", "X")
val rHighPriorityLine: List[String] = List("01", "02", "03")
val rLowPriorityLine: List[String] = List("04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15")
val isNddExtendedList: List[String] = List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")

val bicppctrDF = BICPPCTR.filter($"BICPPCTR" > lit(0) && $"OBJVERS" === lit("A"))
val BCNFACTSPDF = BCNFACTSP.filter(length($"BCNFACTSP") > lit(0))
val BCNFSHPQYDF = BCNFSHPQY.filter(length($"BCNFSHPQY") > lit(0))
val bcnfstagpDF = BCNFSTAGP.filter(length($"BCNFSTAGP") > lit(0))
val MaterialObjectVerADF = MaterialDF.filter($"OBJVERS" === lit("A"))
val DistinctMaterialDF = MaterialDF.filter($"OBJVERS" === lit("A"))
  .join(bof20DF, "MATERIAL")
  .select(MaterialDF.col("BACQADVCD"), $"BADMLDTME", $"BPRODLDTM", $"PROFIT_CTR", $"OBJVERS", $"MATERIAL")
  .dropDuplicates

// Distinct columns to meet ACT_SHIP and BSHIP_QTY Criteria
val distinctBTypeItemCatConfDateDF = bof20DF
  .select(
    $"BORD_TYPE",
    when(length(col("CONF_DATE")) > lit(0), lit("G")).otherwise(lit("")).alias("CONF_DATE_FLAG"),
    $"ITEM_CATEG"
  ).distinct

val distinctReasonActgiItemCategConfDateDF = bof20DF
  .select(
    when(length(col("REASON_REJ")) < lit(1), lit("<>")).otherwise(col("REASON_REJ")).alias("REASON_REJ_CHK"),
    when(length(col("ACT_GI_DTE")) < lit(1), lit("N")).otherwise(lit("Y")).alias("ACT_GI_FLAG"),
    when(length(col("CONF_DATE")) < lit(1), lit("N")).otherwise(lit("Y")).alias("CONF_DATE_FLAG"),
    $"ITEM_CATEG"
  ).distinct

for (c <- distinctBTypeItemCatConfDateDF.columns) {
  println(c)
}

// WindowSpec creations for windowing in ACT_SHIP and BSHIP_QTY transformations
val byBTypeItemCatConfDateOrderByBcnfshpqy = Window
  .partitionBy($"BORD_TYPE", $"ITEM_CATEG", $"CONF_DATE_FLAG")
  .orderBy($"BCNFSHPQY".asc)

val byItemCatReasonActgiConfDateOrderByBcnfactsp = Window
  .partitionBy($"ITEM_CATEG", $"REASON_REJ_CHK", $"ACT_GI_FLAG", $"CONF_DATE_FLAG")
  .orderBy($"BCNFACTSP".asc)

// Julian Date Parsing Function
def julianDateParse(myJulianYDDD: String) = {
  try {
    val sysDate: String = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())
    val sysDateDecade: String = sysDate.substring(0, 3)
    if (sysDateDecade + myJulianYDDD > sysDate) {
      (Integer.parseInt(sysDateDecade) - 1) + myJulianYDDD
    } else sysDateDecade + myJulianYDDD
  } catch {
    case _: Throwable => null
  }
}

val julianDateParseUDF = udf[String, String](julianDateParse)

// Add Days Function
def addDays = udf((rawDate: String, day2Add: Int) => {
  try {
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val c = new GregorianCalendar()

    c.setLenient(false)
    c.set(
      Integer.parseInt(rawDate.substring(0, 4)),
      Integer.parseInt(rawDate.substring(4, 6)),
      Integer.parseInt(rawDate.substring(6, 8))
    )
    c.add(Calendar.DATE, day2Add)
    formatter.format(c.getTime())
  } catch {
    case _: Throwable => null
  }
})

// Data Transformations
val shipQtyfromBcnfshpqy = selectShipQty(BCNFSHPQYDF, distinctBTypeItemCatConfDateDF, byBTypeItemCatConfDateOrderByBcnfshpqy).get
val actShipfromBcnfactsp = selectActShip(BCNFACTSPDF, distinctReasonActgiItemCategConfDateDF, byItemCatReasonActgiConfDateOrderByBcnfactsp).get

val ingestAfterBofo20DF = ingestBeforeBofo20DF
  .alias("bof20DF")
  .filter(length(col("bof20DF.DOC_CATEG")) > lit(0))
  .withColumn(
    "CONF_DATE_FLAG",
    when(length(col("CONF_DATE")) > lit(0), lit("G")).otherwise(lit(""))
  )
  .alias("ingestBeforeBofo20ConfDateFlagDF")
  .join(
    shipQtyfromBcnfshpqy.alias("shipQtyfromBcnfshpqy"),
    col("shipQtyfromBcnfshpqy.ITEM_CATEG") === col("ingestBeforeBofo20ConfDateFlagDF.ITEM_CATEG") &&
      col("shipQtyfromBcnfshpqy.BORD_TYPE") === col("ingestBeforeBofo20ConfDateFlagDF.BORD_TYPE") &&
      col("shipQtyfromBcnfshpqy.CONF_DATE_FLAG") === col("ingestBeforeBofo20ConfDateFlagDF.CONF_DATE_FLAG")
  )
  .withColumn(
    "BSHIP_QTY",
    when(col("BCNFCHR01") === lit("I"), col("GIS_QTY"))
      .otherwise(
        when(col("BCNFCHR01") === lit("G"), col("GR_QTY"))
          .otherwise(
            when(col("BCNFCHR01") === lit("C"), col("CONF_QTY"))
              .otherwise(
                when(col("BCNFCHR01") === lit("D"), col("DLV_QTY"))
                  .otherwise(when(col("BCNFCHR01") === lit("M"), col("BMRA_QTY")))
              )
          )
      )
  )
  .drop(
    "shipQtyfromBcnfshpqy.ITEM_CATEG",
    "shipQtyfromBcnfshpqy.BORD_TYPE",
    "shipQtyfromBcnfshpqy.CONF_DATE_FLAG",
    "BCNFCHR01"
  )
  .withColumn(
    "REASON_REJ_CHK",
    when(length(col("REASON_REJ")) < lit(1), lit("<>")).otherwise(col("REASON_REJ"))
  )
  .withColumn(
    "ACT_GI_FLAG",
    when(length(col("ACT_GI_DTE")) < lit(1), lit("N")).otherwise(lit("Y"))
  )
  .withColumn(
    "CONF_DATE_FLAG",
    when(length(col("CONF_DATE")) < lit(1), lit("N")).otherwise(lit("Y"))
  )
  .alias("ingestBeforeBofo20AllJoinFlagsDF")
  .join(
    actShipfromBcnfactsp.alias("actShipfromBcnfactsp"),
    col("actShipfromBcnfactsp.ITEM_CATEG") === col("ingestBeforeBofo20AllJoinFlagsDF.ITEM_CATEG") &&
      col("actShipfromBcnfactsp.REASON_REJ_CHK") === col("ingestBeforeBofo20AllJoinFlagsDF.REASON_REJ_CHK") &&
      col("actShipfromBcnfactsp.ACT_GI_FLAG") === col("ingestBeforeBofo20AllJoinFlagsDF.ACT_GI_FLAG") &&
      col("actShipfromBcnfactsp.CONF_DATE_FLAG") === col("ingestBeforeBofo20AllJoinFlagsDF.CONF_DATE_FLAG")
  )
  .withColumn("BACT_SHIP", new BofParser().calculateBactShip)
  .withColumn("PSTNG_DATE", dateParseUdf(col("BGR_DATE")))
  .join(
    bcnfstagpDF.alias("bcnfstagpDF"),
    bcnfstagpDF.col("DOC_TYPE") === ingestBeforeBofo20DF.col("DOC_TYPE"),
    "left"
  )
  .drop("bcnfstagpDF.DOC_TYPE")
  .join(
    bicppctrDF.alias("bicppctrDF"),
    ingestBeforeBofo20DF.col("PROFIT_CTR") >= bicppctrDF.col("BLOPRCTR") &&
      ingestBeforeBofo20DF.col("PROFIT_CTR") <= bicppctrDF.col("BHIPRCTR"),
    "left"
  )
  .withColumn(
    "GV_CREATED_ON",
    when(col("bcnfstagpDF.DOC_TYPE").isNull, col("BORDER_DT")).otherwise(col("CREATEDON"))
  )
.withColumn("BTGT_SHIP", new BofParser().calculateBtgtShip)

.withColumn("BTGTSHIP", col("BTGT_SHIP"))
.withColumn("BGR_DATE_TEMP", dateParseUdf(col("BGR_DATE"))).drop(col("BGR_DATE")).withColumnRenamed("BGR_DATE_TEMP","BGR_DATE")
.withColumn("BCREATE_TEMP", dateParseUdf(col("BCREATEDT"))).drop(col("BCREATEDT")).withColumnRenamed("BCREATE_TEMP","BCREATEDT")
.withColumn("DOC_DATE_TEMP", dateParseUdf(col("DOC_DATE"))).drop(col("DOC_DATE")).withColumnRenamed("DOC_DATE_TEMP","DOC_DATE")
.withColumn("SCL_DELDAT_TEMP", dateParseUdf(col("DSCL_DELDAT"))).drop(col("SCL_DELDAT")).withColumnRenamed("SCL_DELDAT_TEMP","SCL_DELDAT")
.select(bof20Bof12Bof15ColsforBof30.head, bof20Bof12Bof15ColsforBof30.tail:_*)
                                              
/** Start BOFO15 **/
val ingestAfterbofo15DF = bof15DF
  .filter(col("DOC_CATEG") =!= lit("C"))
  .filter(col("DOC_CATEG") =!= lit("I"))
  // Does The Record Mode column come in as " " like in FS or come in as null
  // .withColumn("NEW_RECORD_MODE", when(col("REC_DELETD") <=> lit("X") && col("RECORDMODE") <=> lit(""), lit("A"))
  // .otherwise(col("RECORDMODE"))).drop("RECORDMODE").withColumnRenamed("NEW_RECORD_MODE", "RECORDMODE")
  .select(bofo15UsedForBofo30Seq.head, bofo15UsedForBofo30Seq.tail: _*)
/** End BOFO15 **/

/********************************************************** Start Bofo12 **********************************************************/

/* WindowSpec creations for windowing in MILSVC transformations */
val byCUST_GRP3andBDODAACorderedByBCNFMILSP = Window
  .partitionBy($"CUST_GRP3", $"BDODAAC")
  .orderBy($"BCNFMILSP".desc)

val byCUST_GRP3andBDODAACandSOLD_TOorderedByBCNFMILSP = Window
  .partitionBy($"CUST_GRP3", $"SOLD_TO", $"BORGDNUM")
  .orderBy($"BCNFMILSP".desc)

/* Subsets of rule criteria columns for MILSVC transformations */
val distinctCUST_GRP3andBDODAACDF = bof12DF
  .select($"CUST_GRP3", $"BDODAAC")
  .distinct

val distinctCUST_GRP3andBDODAACandSOLD_TODF = bof12DF
  .select($"CUST_GRP3", $"BORGDNUM", $"SOLD_TO")
  .distinct

/* Subsets of rule criteria columns for BMILSVC_1 transformation with BMILSVC_1 */
val distinctCUST_GRP3andBDODAACwithBMILSVC_1DF = selectBmilsvc1(
  distinctCUST_GRP3andBDODAACDF,
  BCNFMILSPDF,
  byCUST_GRP3andBDODAACorderedByBCNFMILSP
).get
distinctCUST_GRP3andBDODAACwithBMILSVC_1DF.show()

val ingestBeforeBofo12DF = joinIngestBofs(bof12DF, ingestAfterbofo15DF, bofo15Keys.split(",")).get

/* After the BMILSVC_1 transformation joined back to bofo12 */
val bof12WithBMILSVC_1 = ingestBeforeBofo12DF
  .join(
    distinctCUST_GRP3andBDODAACwithBMILSVC_1DF,
    distinctCUST_GRP3andBDODAACwithBMILSVC_1DF.col("CUST_GRP3") === bof12DF.col("CUST_GRP3") &&
      distinctCUST_GRP3andBDODAACwithBMILSVC_1DF.col("BDODAAC") === bof12DF.col("BDODAAC"),
    "left"
  )
  .drop(ingestBeforeBofo12DF.col("CUST_GRP3"))
  .drop(ingestBeforeBofo12DF.col("BDODAAC"))

/* Subsets of rule criteria columns for BMILSVC_2 transformation with BMILSVC_2 */
val distinctCUST_GRP3andBDODAACwithBMILSVC_2DF = selectBmilsvc2(
  distinctCUST_GRP3andBDODAACDF,
  BCNFMILSPDF,
  byCUST_GRP3andBDODAACorderedByBCNFMILSP
).get
distinctCUST_GRP3andBDODAACwithBMILSVC_2DF.show()

/* After the BMILSVC_2 transformation joined back to bofo12 */
val bof12WithBMILSVC_2 = bof12WithBMILSVC_1
  .join(
    distinctCUST_GRP3andBDODAACwithBMILSVC_2DF,
    distinctCUST_GRP3andBDODAACwithBMILSVC_2DF.col("CUST_GRP3") === bof12WithBMILSVC_1.col("CUST_GRP3") &&
      distinctCUST_GRP3andBDODAACwithBMILSVC_2DF.col("BDODAAC") === bof12WithBMILSVC_1.col("BDODAAC"),
    "left"
  )
  .drop(bof12WithBMILSVC_1.col("CUST_GRP3"))
  .drop(bof12WithBMILSVC_1.col("BDODAAC"))
bof12WithBMILSVC_2.show(1)

/* Subsets of rule criteria columns for BMILSVC_3 transformation with BMILSVC_3 */
val distinctCUST_GRP3andBDODAACandSOLD_TOwithBMILSVC_3DF = selectBmilsvc3(
  distinctCUST_GRP3andBDODAACandSOLD_TODF,
  BCNFMILSPDF,
  ZPCUSTOMERDF,
  byCUST_GRP3andBDODAACandSOLD_TOorderedByBCNFMILSP
).get
distinctCUST_GRP3andBDODAACandSOLD_TOwithBMILSVC_3DF.show()

/* After the BMILSVC_3 transformation joined back to bofo12 */
val bof12WithBMILSVC_3 = bof12WithBMILSVC_2
  .join(
    distinctCUST_GRP3andBDODAACandSOLD_TOwithBMILSVC_3DF,
    distinctCUST_GRP3andBDODAACandSOLD_TOwithBMILSVC_3DF.col("CUST_GRP3") === bof12WithBMILSVC_2.col("CUST_GRP3") &&
      distinctCUST_GRP3andBDODAACandSOLD_TOwithBMILSVC_3DF.col("BORGDNUM") === bof12WithBMILSVC_2.col("BORGDNUM") &&
      distinctCUST_GRP3andBDODAACandSOLD_TOwithBMILSVC_3DF.col("SOLD_TO") === bof12WithBMILSVC_2.col("SOLD_TO"),
    "left"
  )
  .drop(bof12WithBMILSVC_2.col("CUST_GRP3"))
  .drop(bof12WithBMILSVC_2.col("BORGDNUM"))
  .drop(bof12WithBMILSVC_2.col("SOLD_TO"))
bof12WithBMILSVC_3.show(1)

/* Assign BSTAGRP */
val bof12WithBSTAGRP = selectBstagrp(bof12WithBMILSVC_3, bcnfstagpDF).get
bof12WithBSTAGRP.show(1)



/* 
   Assign BPIIN_13 
    How does BPONR look in the bofo12 file in Spark? Does The BPONBR column come in as "" like in FS or come in as null? 
*/
val bof12Bof15ColsforBof30 = (bofo15UsedForBofo30Seq ++ bofo12UsedForBofo30Seq).distinct

val ingestAfterBofo12v1DF = bof12WithBSTAGRP.withColumn("BPIIN_13", when(isnull(col("BPONBR")) || col("BPONBR") <=> lit(""), substring(col("BPONBR"),1,13)).otherwise(lit("")))
    .withColumn("BSHIP_MON", getFiscalYYYYMMUDF(col("CREATEDON")))
    .withColumn("CALMONTH", getFiscalYYYYMMUDF(col("CREATEDON")))
    .withColumn("OBPARTNER", col("SOLD_TO"))
    //still have to select bof12 first over bof15 with coalesce
    .select(bof12Bof15ColsforBof30.head, bof12Bof15ColsforBof30.tail:_*)
   
                           
ingestAfterBofo12v1DF.show(1)

val ingestBeforeBofo20DF = joinIngestBofs(bof20DF,ingestAfterBofo12v1DF,bofo30Keys.split(",")).get
ingestBeforeBofo20DF.show(1)

/************************************************* End Bofo12 to Bof30 *********************************************************************************/ 


/************************************************* MasterData Used For BOFO20 **************************************************************************/
/* 
   Assign BPIIN_13 
   How does BPONR look in the bofo12 file in Spark? Does The BPONBR column come in as "" like in FS or come in as null? 
*/
val bof12Bof15ColsforBof30 = (bofo15UsedForBofo30Seq ++ bofo12UsedForBofo30Seq).distinct

val ingestAfterBofo12v1DF = bof12WithBSTAGRP
  .withColumn(
    "BPIIN_13",
    when(isnull(col("BPONBR")) || col("BPONBR") <=> lit(""), substring(col("BPONBR"), 1, 13)).otherwise(lit(""))
  )
  .withColumn("BSHIP_MON", getFiscalYYYYMMUDF(col("CREATEDON")))
  .withColumn("CALMONTH", getFiscalYYYYMMUDF(col("CREATEDON")))
  .withColumn("OBPARTNER", col("SOLD_TO"))
  // Still have to select bof12 first over bof15 with coalesce
  .select(bof12Bof15ColsforBof30.head, bof12Bof15ColsforBof30.tail: _*)

ingestAfterBofo12v1DF.show(1)

val ingestBeforeBofo20DF = joinIngestBofs(bof20DF, ingestAfterBofo12v1DF, bofo30Keys.split(",")).get
ingestBeforeBofo20DF.show(1)

/************************************************* End Bofo12 to Bof30 *************************************************/

/************************************************* MasterData Used For BOFO20 ******************************************/

val bcnfactspDF = spark.read
  .format("csv")
  .option("header", "false")
  .option("delimiter", ",")
  .schema(BCNFACTSPSchema)
  .load("/data/bcnfactsp/BCNFACTSP.csv")
  .filter(length($"BCNFACTSP") > lit(0))

val bcnfshpqyDF = spark.read
  .format("csv")
  .option("header", "false")
  .option("delimiter", ",")
  .schema(BCNFSHPQYSchema)
  .load("/data/bcnfshpqy/BCNFSHPQY.csv")
  .filter(length($"BCNFSHPQY") > lit(0))

/*********************************************** End Of MasterData Used For BOFO20 *************************************/

/** Start Bofo20 **/

val bof20Bof12Bof15ColsforBof30 = (bofo20UsedForBofo30Seq ++ bof12Bof15ColsforBof30).distinct

val ingestAfterBofo20DF = ingestBeforeBofo20DF
  .filter(col("DOC_CATEG") =!= lit("C"))
  .filter(col("DOC_CATEG") =!= lit("I"))
  // Find the current massageAndErrorCheckDF method
  // Need verification on PSTNG_DATE column
  .withColumn("BSHIP_QTY", lit("00000000"))
  .withColumn("PSTNG_DATE", dateParseUdf(col("BGR_DATE")))
  .drop("BGR_DATE")
  // Placeholder for BSHIP_QTY
  .withColumn("BSHIP_QTY", dateParseUdf(col("PSTNG_DATE")))
  .select(bof20Bof12Bof15ColsforBof30.head, bof20Bof12Bof15ColsforBof30.tail: _*)

/** End Bofo20 **/

val bof08Bof20Bof12Bof15ColsforBof30 = (bofo08UsedForBofo30Seq ++ bof20Bof12Bof15ColsforBof30).distinct
/** Start Bofo08 **/

//val ingestAfterbofo08DF = bof08DF.filter(col("DOC_CATEG") =!= lit("C"))
//                                 .filter(col("DOC_CATEG") =!= lit("I"))
//                                 .select(bof08Bof20Bof12Bof15ColsforBof30.head, bof08Bof20Bof12Bof15ColsforBof30.tail:_*)

/** End Bofo08 **/ 
/*
+---------+-------+---------+
|CUST_GRP3|BDODAAC|BMILSVC_2|
+---------+-------+---------+
|       MD|    CAB|     null|
|     EPMD|    AAC|     null|
|     EPMD|    CCB|     null|
|       MD|    CDA|     null|
|      NWA|    BCA|     null|
|      NBN|    AAC|     null|
|      NWA|    ACC|     null|
|      NWA|    CDC|     null|
|       MD|    DAC|     null|
|      BBD|    BCC|     null|
|     EPMD|    AAB|     null|
|       MD|    CDC|     null|
|      NBN|    AAB|     null|
|      NWA|    DCD|     null|
+---------+-------+---------+


+----------+----------+----------+----------+----------+----------+--------+----------+----------+-------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+---------+-------+---------+
|DOC_NUMBER|S_ORD_ITEM|   STS_ITM|  STS_BILL|   STS_PRC|   STS_DEL|DOC_TYPE| COMP_CODE|LOC_CURRCY|SOLD_TO| CUST_GRP1| CUST_GRP2| CUST_GRP4| CUST_GRP5| DEL_BLOCK|DOC_CATEG| SALES_OFF| SALES_GRP|  SALESORG|DISTR_CHAN|REASON_REJ|     CH_ON|ORDER_PROB| GROSS_WGT|  BWAPPLNM|PROCESSKEY|     BATCH| EXCHG_CRD|    EANUPC|CREATEDON| CREATEDBY| CREA_TIME|BILBLK_ITM|UNIT_OF_WT|CML_CF_QTY|CML_CD_QTY| COND_UNIT| SALESDEAL|COND_PR_UN|CML_OR_QTY|SUBTOTAL_1|SUBTOTAL_2|SUBTOTAL_3|SUBTOTAL_4|SUBTOTAL_5|SUBTOTAL_6|MIN_DL_QTY|  STOR_LOC|REQDEL_QTY|MATL_GROUP| MATERIAL| MAT_ENTRD|  BASE_UOM|MATL_GRP_1|MATL_GRP_2|MATL_GRP_3|MATL_GRP_4|MATL_GRP_5| TAX_VALUE| NET_PRICE| NET_VALUE| NET_WT_AP|UNLD_PT_WE|BILLTOPRTY|     PAYER|   SHIP_TO| PROD_HIER| FORWAGENT|ITEM_CATEG|SALESEMPLY|     ROUTE| SHIP_STCK|  DIVISION| STAT_DATE|EXCHG_STAT|SUB_REASON|   BND_IND|  UPPR_BND| DENOMINTR| NUMERATOR|DENOMINTRZ|NUMERATORZ|  LOWR_BND| ST_UP_DTE| REFER_DOC| REFER_ITM|PRVDOC_CTG|VOLUMEUNIT| VOLUME_AP|SALES_UNIT|SHIP_POINT|DOC_CURRCY|      COST|     PLANT| TARGET_QU|TARGET_QTY|TARG_VALUE|   BADV_CD|  BBILL_AD| BILL_TYPE|  BBO_TYPE|  BBILL_ST|  BCAGE_CD|   BCNLDDT|     B_CAS| BCOMMODTY|  BCONDITN|  BCONT_LN| BCNTRLNBR|     B_CRM|  BDISTRIB|   BDEMAND| BDMDPLANT|  BDOCIDCD| BTGT_SHIP| BEXC_INFO|     B_SST|BFMS_PRG|  BFUND_CD|   BINVCNT|      BLOA|    BCLSSA|   BPO_NUM| BPO_ITMNO| BMEDST_CD|  BMGMT_CD|     BMIPR|  BMIPRNBR|    BMODDT|   BNS_RDD|  BORGDNUM|  BOWNRSHP| BPRIUSRID|   BPRI_CD|  BPROJ_CD|    BPONBR|  BPURPRIC|  BPURPOSE|  BRCD_NUM| BCONDCTRL| BRETNTQTY|  BRICFROM| BSTDDLVDT|  BSUFX_CD|  BSHIPNBR|   BSIG_CD| BSPLPGMRQ|  BSUPP_ST|  BSUPPADR|SALES_DIST| SERV_DATE| BILL_DATE| INCOTERMS|INCOTERMS2|CUST_GROUP|ACCNT_ASGN|EXCHG_RATE|TRANS_DATE|PRICE_DATE|  RT_PROMO|   PRODCAT|ORDER_CURR|  DIV_HEAD|DOC_CATEGR| WBS_ELEMT| ORD_ITEMS| FISCVARNT|PROFIT_CTR|   CO_AREA|  BCONVERT| BCUSTPODT|    BBSTNK|REJECTN_ST| QUOT_FROM|ORD_REASON|   QUOT_TO|BILL_BLOCK|    BAUDAT| RATE_TYPE| STAT_CURR|    BKDMAT| BMFRPRTNO| BPROJ_NUM| BMFR_CAGE| BMFR_NAME|   DOCTYPE| BITMCATGP|DSDEL_DATE| BDISCP_CD| BMATRCVDT|  BMRA_QTY| BDISCPQTY| BORDER_DT|BOFBNSRDD| BOFITMCAT|  BOFPRICD|    BZZESD|   BRIC_TO|      BMOQ| BHDRCRTDT|    BZZSSC|  BZZDMDDN|  BZZDMDSC| BZZODNPST|   BZZJOKO|  BZZUSECD| BZZCHRNCD| BZZPRIREF| BZZINSPCD| BZZPICKLS| BZZMATAQC| BZZRICSOS| BZZUTILCD| BZREPMATL|    BUEPOS|    BSTLNR| BZZSRC_CD|  BEARCDTE|  BRB_DTID| BFREXP_DT| BZZCUR_DT| BFLPICKUP| BCONBIDPR| BTRQTYSUN| BCONISFFL|  DLV_PRIO|  BSHRTTXT| BGFN_CLIN| BGFM_CONT| BGFM_CALL|  BGFM_MDN| BPS_POPST| BPS_POPEN| BMIPR_NBR|  BTPDDATE|   BTPDQTY|DLV_STSO|DLV_STS|LW_GISTS|LST_A_GD|BMATL_BLK|BMILSVC_1|CUST_GRP3|BDODAAC|BMILSVC_2|
+----------+----------+----------+----------+----------+----------+--------+----------+----------+-------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+---------+-------+---------+
|    400000|      0010|exArXn3fZG|jq8hdWwdCm|jsPFgyt32w|kM3QBTi95W|    BLUE|MMUp90d9JG|1Gc8bWZFQT|  CUST3|lkT4ocJPF1|lOYxjDRzre|QRU651lXT2|xkYA1bo7AL|BuM2MWg2b5|        E|P5PqI2eR0Q|Uut95kCVAt|C6pVsO6a2R|gi2e27iiLD|3aFVwkPthj|9dpijP4XFo|kcwliMn54I|ECHzK367Pk|FtSPWY3DFl|UTrNqgVJsc|DTADEJPmUl|6bjLw3DBJQ|XA4qf686L3| 20161201|iajUp5D2Lt|D5n5Q5udMi|pBld8kILTY|R9n33DxyxJ|PgWPWls7PZ|US2ubSkRYR|S9ZFC1Kwlb|jPBnwjBKxM|99Bn8afXQz|MN5kLeDtIs|k9uAwHv0pg|zkVJTWxnNe|l6tbsN06FX|4KwU8X50Dx|1XSFm65jRb|qmob275ryH|DeH6rvTRrN|9vknXjx5AO|cXVjwEqWrw|9HYSfVJ7Lo|STYROFOAM|PJKYsJksrT|cmQkukjTsM|EKE5nTfNRH|ojW5n6uKOA|SVc4YtbGqg|JqjnWeXChZ|UkrbZocq9k|ZW0QLSl0GF|ZcUrFQzgwD|VqPddc4pCw|595i5xs6fH|HkwFqRKvDA|mY43wyGTOt|PPqb8vXxKu|7zimF0nz7G|E5QgCVPFIk|ckMbCRYs0o|      ZSTO|iYCZ62ATXo|ccuJaU7Sav|HbrqlnRhOT|nCXcDAL1Td|nrC4bbe4Rp|EzJob6wMcy|UEHQblGqB9|KzDxq4nQrV|GwK9lx4cEl|WnzOS1C2Pl|luMnxcOPCx|vEWtFsrexu|ry5ZuiJLKo|bACfZq3eic|YBZYD5Aih5|lP6Kgt3U9S|VtgpPNoYL8|kzjhowey5C|VTtStSJbVG|AB7TqarAmi|cRx1ztXdbC|OCzGXL4IgN|FpXSnArUsK|Q6UZw9vRpC|b0LrdjuERy|H702jmZPul|nQy2RYRpB4|krNGmrmsoe|gwXgjlI01j|5QLP7VqNa7|CRpmWYFLrm|4Onh0XW1h5|78SoN3xFwr|HZJevY3aOH|AQbzLoYlUh|zG3euhaR9k|dq4V6g6FIQ|4P8qLzglU1|Uv8GwuNLYq|opgW0cMNSd|eKLdsjcTPk|sjFvPoE5D2|oGYBVG55iW|4JjbWlGRUP|5QkL6KNDDN|l9l6KnwPOK|tPPp7MPZMP|uKpD5xG7An|       D|WYhwy0tnOM|ueC6OFPtny|BEJ2fu6EOu|j5kC8Q9rVA|sBwtGoXV1P|covOGZPOO8|UMwBPJC0lM|VuqvbVOMcp|49IvVilUeE|b2sdCSK65M|MTE7EqIz4l|CCoCHqBpuE|zYimehL8vD|3BMqIZH8E4|WjWYMpBQb8|ZheIMl0WtS|QkAiFlnDuE|7XMjUAyfOm|NTsR0XOrtr|FfhPpU7joV|y4WlaN4gDb|WHagNwjwxf|4gRLieKXn3|zYGvyPT50S|8nRgqClAN1|pqNppjaRPk|Lnm84SSqvE|sZCc6GFd7y|qv2cjz2hbc|beDaOp7vcj|WOlzUE46tB|Pwyf1m2RwT|WhpQiMXfYo|UJ6Kz0MUfH|I7v9ynjEo3|rC02bR4N9u|Pqgx1ndARH|wHTMpCCvZT|uTuA7gQo2W|VeRmaBxL5a|MDHPkmkBpS|AowCRmGl7L|dlLFyKT6Vk|HDOzjFPf01|a1qofriQ5s|3C1kthLXwY|1S4p0Z3Ko2|bkTQI2yocs|UkI7CwHvas|FCXoGWaiQY|h0S0j5T7GF|Vg8npifW1d|EbfYwEhelh|x2mB3fg9za|65N9mGfcAy|EacMO7vyFL|kjsoEnCZap|tLCW1z43u2|TSRMOyl6S4|WKmlBiB9VF|pdQ1poJ5FE|nnwrO3PeJY|4wlaEJZ3Rj|BBnyYkX4lh|C7dTJYRskK|NyPkFJxFNu|VLSg1BWzUO|2Pm0tzGvyh|Ib8HpvwA5b|kkiWcHPOtY|cou1unvPBc|5TKYOK89wb|P4JukVMyIc|tmkOz8LzxC|hVlRe3tgNY| 20170201|50QYTSQoyr|PvBgkrILVm|mUD5DxUACw|L1IDCyYWL7|0JRmsl2QIV|1i8clltMU0|TRrjRVimfv|Q9QD6oVwjx|joHB7QT5nQ|8US4q5O6ba|w9samzka5u|zpl1ZRq3v1|ZKKhkl2r5f|aUgz2I6Ozz|TwO6JrPFRn|SRJZetyhH8|cp35iIKbwr|iJSidIIvPS|A3cyhWJBEk|m0llT53IIk|d9I9hvQDA1|AZkdpmqrEA|zGy4yUgiTh|J2i1tI3tNJ|tgXCAKUnzr|4YiZRslKVi|9ZrXZnlvxi|3XufmiceCE|QDZ4a7HM08|S3Sy26t4YU|Mo9ySkFSq2|99kt9HtC1z|DWNflHcGhV|YQ5giBJxmI|vZhcUo3RoH|b3mswCI8p6|5dDgSERyTH|Zp4bJwGAAn|TypSJW6IHT|DKOhWEu6A9|OOGq6sj7BZ|1OjZ8uqkQx|    null|   null|    null|    null|     null|      AXA|       MD|    CAB|     null|
+----------+----------+----------+----------+----------+----------+--------+----------+----------+-------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+---------+-------+---------+
only showing top 1 row

+---------+----------+-------+---------+
|CUST_GRP3|  BORGDNUM|SOLD_TO|BMILSVC_3|
+---------+----------+-------+---------+
|     EPMD|S6cPjDDYWg|  CUST2|     null|
|      NWA|X9HMj06xi9|  CUST5|     null|
|       MD|zYimehL8vD|  CUST3|     null|
|      BBD|lWrRvx5T1c|  CUST1|     null|
|      NWA|RxhqfOrQxF|  CUST4|     null|
|      NBN|lOzLdXgVL3|  CUST3|     null|
|      NWA|gZL4kT6j32|  CUST1|     null|
|       MD|FDhfD9TUrt|  CUST5|     null|
|       MD|Q6l0xhQczJ|  CUST2|     null|
|       MD|3BAhioove2|  CUST3|     null|
|      NBN|y0Ii2ZAbhp|  CUST3|     null|
|      NWA|dZmEvoF5im|  CUST1|     null|
|     EPMD|OkVt4dTplF|  CUST4|     null|
|     EPMD|DrD4LNoP30|  CUST4|     null|
|     EPMD|F4mFOuLhxO|  CUST4|     null|
+---------+----------+-------+---------+

+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+-------+---------+---------+----------+-------+---------+
|DOC_NUMBER|S_ORD_ITEM|   STS_ITM|  STS_BILL|   STS_PRC|   STS_DEL|DOC_TYPE| COMP_CODE|LOC_CURRCY| CUST_GRP1| CUST_GRP2| CUST_GRP4| CUST_GRP5| DEL_BLOCK|DOC_CATEG| SALES_OFF| SALES_GRP|  SALESORG|DISTR_CHAN|REASON_REJ|     CH_ON|ORDER_PROB| GROSS_WGT|  BWAPPLNM|PROCESSKEY|     BATCH| EXCHG_CRD|    EANUPC|CREATEDON| CREATEDBY| CREA_TIME|BILBLK_ITM|UNIT_OF_WT|CML_CF_QTY|CML_CD_QTY| COND_UNIT| SALESDEAL|COND_PR_UN|CML_OR_QTY|SUBTOTAL_1|SUBTOTAL_2|SUBTOTAL_3|SUBTOTAL_4|SUBTOTAL_5|SUBTOTAL_6|MIN_DL_QTY|  STOR_LOC|REQDEL_QTY|MATL_GROUP|MATERIAL| MAT_ENTRD|  BASE_UOM|MATL_GRP_1|MATL_GRP_2|MATL_GRP_3|MATL_GRP_4|MATL_GRP_5| TAX_VALUE| NET_PRICE| NET_VALUE| NET_WT_AP|UNLD_PT_WE|BILLTOPRTY|     PAYER|   SHIP_TO| PROD_HIER| FORWAGENT|ITEM_CATEG|SALESEMPLY|     ROUTE| SHIP_STCK|  DIVISION| STAT_DATE|EXCHG_STAT|SUB_REASON|   BND_IND|  UPPR_BND| DENOMINTR| NUMERATOR|DENOMINTRZ|NUMERATORZ|  LOWR_BND| ST_UP_DTE| REFER_DOC| REFER_ITM|PRVDOC_CTG|VOLUMEUNIT| VOLUME_AP|SALES_UNIT|SHIP_POINT|DOC_CURRCY|      COST|     PLANT| TARGET_QU|TARGET_QTY|TARG_VALUE|   BADV_CD|  BBILL_AD| BILL_TYPE|  BBO_TYPE|  BBILL_ST|  BCAGE_CD|   BCNLDDT|     B_CAS| BCOMMODTY|  BCONDITN|  BCONT_LN| BCNTRLNBR|     B_CRM|  BDISTRIB|   BDEMAND| BDMDPLANT|  BDOCIDCD| BTGT_SHIP| BEXC_INFO|     B_SST|BFMS_PRG|  BFUND_CD|   BINVCNT|      BLOA|    BCLSSA|   BPO_NUM| BPO_ITMNO| BMEDST_CD|  BMGMT_CD|     BMIPR|  BMIPRNBR|    BMODDT|   BNS_RDD|  BOWNRSHP| BPRIUSRID|   BPRI_CD|  BPROJ_CD|    BPONBR|  BPURPRIC|  BPURPOSE|  BRCD_NUM| BCONDCTRL| BRETNTQTY|  BRICFROM| BSTDDLVDT|  BSUFX_CD|  BSHIPNBR|   BSIG_CD| BSPLPGMRQ|  BSUPP_ST|  BSUPPADR|SALES_DIST| SERV_DATE| BILL_DATE| INCOTERMS|INCOTERMS2|CUST_GROUP|ACCNT_ASGN|EXCHG_RATE|TRANS_DATE|PRICE_DATE|  RT_PROMO|   PRODCAT|ORDER_CURR|  DIV_HEAD|DOC_CATEGR| WBS_ELEMT| ORD_ITEMS| FISCVARNT|PROFIT_CTR|   CO_AREA|  BCONVERT| BCUSTPODT|    BBSTNK|REJECTN_ST| QUOT_FROM|ORD_REASON|   QUOT_TO|BILL_BLOCK|    BAUDAT| RATE_TYPE| STAT_CURR|    BKDMAT| BMFRPRTNO| BPROJ_NUM| BMFR_CAGE| BMFR_NAME|   DOCTYPE| BITMCATGP|DSDEL_DATE| BDISCP_CD| BMATRCVDT|  BMRA_QTY| BDISCPQTY| BORDER_DT|BOFBNSRDD| BOFITMCAT|  BOFPRICD|    BZZESD|   BRIC_TO|      BMOQ| BHDRCRTDT|    BZZSSC|  BZZDMDDN|  BZZDMDSC| BZZODNPST|   BZZJOKO|  BZZUSECD| BZZCHRNCD| BZZPRIREF| BZZINSPCD| BZZPICKLS| BZZMATAQC| BZZRICSOS| BZZUTILCD| BZREPMATL|    BUEPOS|    BSTLNR| BZZSRC_CD|  BEARCDTE|  BRB_DTID| BFREXP_DT| BZZCUR_DT| BFLPICKUP| BCONBIDPR| BTRQTYSUN| BCONISFFL|  DLV_PRIO|  BSHRTTXT| BGFN_CLIN| BGFM_CONT| BGFM_CALL|  BGFM_MDN| BPS_POPST| BPS_POPEN| BMIPR_NBR|  BTPDDATE|   BTPDQTY|DLV_STSO|DLV_STS|LW_GISTS|LST_A_GD|BMATL_BLK|BMILSVC_1|BDODAAC|BMILSVC_2|CUST_GRP3|  BORGDNUM|SOLD_TO|BMILSVC_3|
+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+-------+---------+---------+----------+-------+---------+
|    200000|      0010|lc9sGKbQm9|t4vBwumadr|0A5hPam6LP|gpIFuNwPon|    KIWI|3zR4agwCgr|71Sb5ZdUau|pl7I8woq3h|Y3cBjITWMY|0wM2LqPX7U|HVmMVeyDRo|StHqiniLxc|        E|nxaZPg7htJ|Y5cVS9bgds|2zBArYK3T1|NQ1srApQUR|uK6NZknNjZ|7En2I8FRnV|oTK8F86zbN|OLcIJJQX1U|UCbsWl99Al|8vnDrQ1gqf|4v9jvwUqyo|HyOY9HrUD6|2X73DLUdlH| 20141010|vXQRonTVml|hMl3hxZPok|Z0PRWGySX3|8BgR9JmAvK|bKXgvZwZRE|t4QiQ7ryDR|PFQT56uDxp|cq0rJpjeOn|EpnoN99n8S|cFV5FkWVnI|I8r4gBjZ7r|uSQ7SEhjhA|dronLxU9ie|kzviMW6f6t|ZndLy7UMpF|j9Gwnva0Ec|V0LeQsR7Os|hmOUdl9GWD|8HlcdNKdUm|opwVJjmwwu|ALUMINUM|Li4KRPESFc|sYXCeMln59|yhtyUQmBjy|fGDkXd0Isr|NyhQJh7rFP|zNavRPRoyf|OfCZtAsbVt|0wXsQ34U91|2ntx5Dpajp|mHhUSXNuKH|bNEioqBhRe|uzIzcr3qT8|gXkFeGUnxk|FiWWz9V6T8|qRqJ5bu503|Lk1PipZW2k|cebiF9KXnA|      ZTVC|jh0jIWexqW|HbellxvSs6|w842fqvL66|nRxQZy8LVE|mKyVNBTtcf|x7ilXF9jZn|wd7ZZxGgiK|ALXFDGGCTk|2Ob4d6Njgs|YIL2s24rW1|N7Kq8fhPt7|HZdi8GTdEg|q7CufBJP53|xPREARpwro|jlbNc0NxoV|Ac4SCuEbeQ|lTo0h626uP|CPNZrmdsHl|xpWuNLXLqi|Yj8orkYZFD|sJef7aYIs1|opqa42zowg|rO5skSF75J|FuDQSP661W|CB3qHf5u46|czB2XyoaJC|xdDc9EiqrC|jAjfbfmbqp|mXPegZAY0G|NWCM4SeHYU|KCoDj7VyxX|tS58eBgY9K|lr07ezd1To|GhElZgXgyd|mwiNSj1otK|sgvRdTQuQq|KViq8vsC3a|9dtNIuQRC9|96Yl6Ixl52|WaPqh0kpd6|D7MC0zzOHi|jZhRvjbgpi|mIDbn8vnpV|If2tQDfNJU|D1OkaS9gPh|lT94ZX6G79|SaeWvVqyMq|j0hCGB5X9C|       A|LUGTKgaHCO|owXAB2N5wc|GoXJ6Z9Ruk|OV6yq0qjoO|sbYao1ipdU|o5Guq80w3s|X6DliLseS2|UVYTpgZ3ju|Qayb6lirtj|pWtNHZhPIJ|abgEQtVtOs|HwMDBOeYKe|E8n08dPUIc|nOtnc0ubtG|7bYEvOiunX|lYcT9ZDl0r|4w3iK29JQH|ns94yF1l6F|ImpgiE0k05|ssGQ6aJYeP|Akkpzdc36M|VACfhBdyqp|bGpT6X45kf|1hsC9NgKif|kjpQGhVKTU|8gVs6d2uej|CGbaVfYuUg|Lwz0dv3iTj|cCVGNbOpu2|27YVhPTW5G|wSIP4LQue0|pEUo2ZmC3c|QIyTBY1Toy|V8IZkhAB3V|b4hKhtZm92|qeFoLGGqEj|TPISFpSPTa|q1V7nDVpKn|1LM0PxTbOK|rcyPQfZSyk|aMBn5m3afR|TjK9UkQxUK|fJirorUFYn|7X3LglwblI|dPB2vnkymM|7NsT0xEo0t|RairAnmVf2|1J3lJXHCjv|pPMcy2Av02|S1kWVT0Cv3|oparpeq3Pe|KhK0DtBMsk|QjPBUIAIhd|FLy7qdH7oo|kU78PEeavn|3xDhDHT0g2|ysnDurRYuw|zmY9Sl20qX|NvGwpt8DL3|rysV9zn6kD|GZLPEBmhty|nMHfcOc6W1|kJ5SqjrsGN|QzbHcduefF|ke51wRXcCI|2N30A3nqyn|zALzFGR06P|x8dezxhigf|z1W1WPa5jo|Q0fDmO2v0C|qnssrJpQRX|yhjKqB0fhp|NTx8j8GsaG|RhYKuiKYZL| 20160201|2fzP3ltgbV|I9TABWqCNI|FzvRgteSdg|WrES5lETTj|rAZP8biv6L|qKXNBvgwpV|eO6o7qiX7M|w3tEhhOw9o|FJLOuVKJhC|IZn3ZhFJtL|YBMbpT1aWx|JoVYHVB2cQ|ktBZ5s1CIu|ZrLaQ2BdSU|Jxke0boO1N|KoJsjdFnw0|0UlCSNgtjl|lqUWg5AdXz|sZQowfQrts|9TZdBE3TB3|oD3RkAhYdR|Ch0QilDpcZ|feOl6sx5U6|hxIPB2S6ny|xgmWhuneW1|6D234PawEn|3iLcSCEKIq|GVHQeuKV3o|dFXXtJTcp3|20KfwsrHIp|78AeNA1uyr|nejmcUiTNY|1EPuA6OM7q|yKXTRsO0Ma|ZNtj6R6Mbh|zXM0K4JPLH|RGEZY9b0fB|QjdAR3hDed|sPLX0kli9o|NXPI7qBq4n|s8wIq6rN9f|T7eBw9TWNW|    null|   null|    null|    null|     null|      DAA|    DCD|     null|      NWA|gZL4kT6j32|  CUST1|     null|
+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+-------+---------+---------+----------+-------+---------+
only showing top 1 row

+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+-------+---------+---------+----------+-------+---------+----------+
|DOC_NUMBER|S_ORD_ITEM|   STS_ITM|  STS_BILL|   STS_PRC|   STS_DEL|DOC_TYPE| COMP_CODE|LOC_CURRCY| CUST_GRP1| CUST_GRP2| CUST_GRP4| CUST_GRP5| DEL_BLOCK|DOC_CATEG| SALES_OFF| SALES_GRP|  SALESORG|DISTR_CHAN|REASON_REJ|     CH_ON|ORDER_PROB| GROSS_WGT|  BWAPPLNM|PROCESSKEY|     BATCH| EXCHG_CRD|    EANUPC|CREATEDON| CREATEDBY| CREA_TIME|BILBLK_ITM|UNIT_OF_WT|CML_CF_QTY|CML_CD_QTY| COND_UNIT| SALESDEAL|COND_PR_UN|CML_OR_QTY|SUBTOTAL_1|SUBTOTAL_2|SUBTOTAL_3|SUBTOTAL_4|SUBTOTAL_5|SUBTOTAL_6|MIN_DL_QTY|  STOR_LOC|REQDEL_QTY|MATL_GROUP|MATERIAL| MAT_ENTRD|  BASE_UOM|MATL_GRP_1|MATL_GRP_2|MATL_GRP_3|MATL_GRP_4|MATL_GRP_5| TAX_VALUE| NET_PRICE| NET_VALUE| NET_WT_AP|UNLD_PT_WE|BILLTOPRTY|     PAYER|   SHIP_TO| PROD_HIER| FORWAGENT|ITEM_CATEG|SALESEMPLY|     ROUTE| SHIP_STCK|  DIVISION| STAT_DATE|EXCHG_STAT|SUB_REASON|   BND_IND|  UPPR_BND| DENOMINTR| NUMERATOR|DENOMINTRZ|NUMERATORZ|  LOWR_BND| ST_UP_DTE| REFER_DOC| REFER_ITM|PRVDOC_CTG|VOLUMEUNIT| VOLUME_AP|SALES_UNIT|SHIP_POINT|DOC_CURRCY|      COST|     PLANT| TARGET_QU|TARGET_QTY|TARG_VALUE|   BADV_CD|  BBILL_AD| BILL_TYPE|  BBO_TYPE|  BBILL_ST|  BCAGE_CD|   BCNLDDT|     B_CAS| BCOMMODTY|  BCONDITN|  BCONT_LN| BCNTRLNBR|     B_CRM|  BDISTRIB|   BDEMAND| BDMDPLANT|  BDOCIDCD| BTGT_SHIP| BEXC_INFO|     B_SST|BFMS_PRG|  BFUND_CD|   BINVCNT|      BLOA|    BCLSSA|   BPO_NUM| BPO_ITMNO| BMEDST_CD|  BMGMT_CD|     BMIPR|  BMIPRNBR|    BMODDT|   BNS_RDD|  BOWNRSHP| BPRIUSRID|   BPRI_CD|  BPROJ_CD|    BPONBR|  BPURPRIC|  BPURPOSE|  BRCD_NUM| BCONDCTRL| BRETNTQTY|  BRICFROM| BSTDDLVDT|  BSUFX_CD|  BSHIPNBR|   BSIG_CD| BSPLPGMRQ|  BSUPP_ST|  BSUPPADR|SALES_DIST| SERV_DATE| BILL_DATE| INCOTERMS|INCOTERMS2|CUST_GROUP|ACCNT_ASGN|EXCHG_RATE|TRANS_DATE|PRICE_DATE|  RT_PROMO|   PRODCAT|ORDER_CURR|  DIV_HEAD|DOC_CATEGR| WBS_ELEMT| ORD_ITEMS| FISCVARNT|PROFIT_CTR|   CO_AREA|  BCONVERT| BCUSTPODT|    BBSTNK|REJECTN_ST| QUOT_FROM|ORD_REASON|   QUOT_TO|BILL_BLOCK|    BAUDAT| RATE_TYPE| STAT_CURR|    BKDMAT| BMFRPRTNO| BPROJ_NUM| BMFR_CAGE| BMFR_NAME|   DOCTYPE| BITMCATGP|DSDEL_DATE| BDISCP_CD| BMATRCVDT|  BMRA_QTY| BDISCPQTY| BORDER_DT|BOFBNSRDD| BOFITMCAT|  BOFPRICD|    BZZESD|   BRIC_TO|      BMOQ| BHDRCRTDT|    BZZSSC|  BZZDMDDN|  BZZDMDSC| BZZODNPST|   BZZJOKO|  BZZUSECD| BZZCHRNCD| BZZPRIREF| BZZINSPCD| BZZPICKLS| BZZMATAQC| BZZRICSOS| BZZUTILCD| BZREPMATL|    BUEPOS|    BSTLNR| BZZSRC_CD|  BEARCDTE|  BRB_DTID| BFREXP_DT| BZZCUR_DT| BFLPICKUP| BCONBIDPR| BTRQTYSUN| BCONISFFL|  DLV_PRIO|  BSHRTTXT| BGFN_CLIN| BGFM_CONT| BGFM_CALL|  BGFM_MDN| BPS_POPST| BPS_POPEN| BMIPR_NBR|  BTPDDATE|   BTPDQTY|DLV_STSO|DLV_STS|LW_GISTS|LST_A_GD|BMATL_BLK|BMILSVC_1|BDODAAC|BMILSVC_2|CUST_GRP3|  BORGDNUM|SOLD_TO|BMILSVC_3|   BSTAGRP|
+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+-------+---------+---------+----------+-------+---------+----------+
|    200000|      0010|lc9sGKbQm9|t4vBwumadr|0A5hPam6LP|gpIFuNwPon|    KIWI|3zR4agwCgr|71Sb5ZdUau|pl7I8woq3h|Y3cBjITWMY|0wM2LqPX7U|HVmMVeyDRo|StHqiniLxc|        E|nxaZPg7htJ|Y5cVS9bgds|2zBArYK3T1|NQ1srApQUR|uK6NZknNjZ|7En2I8FRnV|oTK8F86zbN|OLcIJJQX1U|UCbsWl99Al|8vnDrQ1gqf|4v9jvwUqyo|HyOY9HrUD6|2X73DLUdlH| 20141010|vXQRonTVml|hMl3hxZPok|Z0PRWGySX3|8BgR9JmAvK|bKXgvZwZRE|t4QiQ7ryDR|PFQT56uDxp|cq0rJpjeOn|EpnoN99n8S|cFV5FkWVnI|I8r4gBjZ7r|uSQ7SEhjhA|dronLxU9ie|kzviMW6f6t|ZndLy7UMpF|j9Gwnva0Ec|V0LeQsR7Os|hmOUdl9GWD|8HlcdNKdUm|opwVJjmwwu|ALUMINUM|Li4KRPESFc|sYXCeMln59|yhtyUQmBjy|fGDkXd0Isr|NyhQJh7rFP|zNavRPRoyf|OfCZtAsbVt|0wXsQ34U91|2ntx5Dpajp|mHhUSXNuKH|bNEioqBhRe|uzIzcr3qT8|gXkFeGUnxk|FiWWz9V6T8|qRqJ5bu503|Lk1PipZW2k|cebiF9KXnA|      ZTVC|jh0jIWexqW|HbellxvSs6|w842fqvL66|nRxQZy8LVE|mKyVNBTtcf|x7ilXF9jZn|wd7ZZxGgiK|ALXFDGGCTk|2Ob4d6Njgs|YIL2s24rW1|N7Kq8fhPt7|HZdi8GTdEg|q7CufBJP53|xPREARpwro|jlbNc0NxoV|Ac4SCuEbeQ|lTo0h626uP|CPNZrmdsHl|xpWuNLXLqi|Yj8orkYZFD|sJef7aYIs1|opqa42zowg|rO5skSF75J|FuDQSP661W|CB3qHf5u46|czB2XyoaJC|xdDc9EiqrC|jAjfbfmbqp|mXPegZAY0G|NWCM4SeHYU|KCoDj7VyxX|tS58eBgY9K|lr07ezd1To|GhElZgXgyd|mwiNSj1otK|sgvRdTQuQq|KViq8vsC3a|9dtNIuQRC9|96Yl6Ixl52|WaPqh0kpd6|D7MC0zzOHi|jZhRvjbgpi|mIDbn8vnpV|If2tQDfNJU|D1OkaS9gPh|lT94ZX6G79|SaeWvVqyMq|j0hCGB5X9C|       A|LUGTKgaHCO|owXAB2N5wc|GoXJ6Z9Ruk|OV6yq0qjoO|sbYao1ipdU|o5Guq80w3s|X6DliLseS2|UVYTpgZ3ju|Qayb6lirtj|pWtNHZhPIJ|abgEQtVtOs|HwMDBOeYKe|E8n08dPUIc|nOtnc0ubtG|7bYEvOiunX|lYcT9ZDl0r|4w3iK29JQH|ns94yF1l6F|ImpgiE0k05|ssGQ6aJYeP|Akkpzdc36M|VACfhBdyqp|bGpT6X45kf|1hsC9NgKif|kjpQGhVKTU|8gVs6d2uej|CGbaVfYuUg|Lwz0dv3iTj|cCVGNbOpu2|27YVhPTW5G|wSIP4LQue0|pEUo2ZmC3c|QIyTBY1Toy|V8IZkhAB3V|b4hKhtZm92|qeFoLGGqEj|TPISFpSPTa|q1V7nDVpKn|1LM0PxTbOK|rcyPQfZSyk|aMBn5m3afR|TjK9UkQxUK|fJirorUFYn|7X3LglwblI|dPB2vnkymM|7NsT0xEo0t|RairAnmVf2|1J3lJXHCjv|pPMcy2Av02|S1kWVT0Cv3|oparpeq3Pe|KhK0DtBMsk|QjPBUIAIhd|FLy7qdH7oo|kU78PEeavn|3xDhDHT0g2|ysnDurRYuw|zmY9Sl20qX|NvGwpt8DL3|rysV9zn6kD|GZLPEBmhty|nMHfcOc6W1|kJ5SqjrsGN|QzbHcduefF|ke51wRXcCI|2N30A3nqyn|zALzFGR06P|x8dezxhigf|z1W1WPa5jo|Q0fDmO2v0C|qnssrJpQRX|yhjKqB0fhp|NTx8j8GsaG|RhYKuiKYZL| 20160201|2fzP3ltgbV|I9TABWqCNI|FzvRgteSdg|WrES5lETTj|rAZP8biv6L|qKXNBvgwpV|eO6o7qiX7M|w3tEhhOw9o|FJLOuVKJhC|IZn3ZhFJtL|YBMbpT1aWx|JoVYHVB2cQ|ktBZ5s1CIu|ZrLaQ2BdSU|Jxke0boO1N|KoJsjdFnw0|0UlCSNgtjl|lqUWg5AdXz|sZQowfQrts|9TZdBE3TB3|oD3RkAhYdR|Ch0QilDpcZ|feOl6sx5U6|hxIPB2S6ny|xgmWhuneW1|6D234PawEn|3iLcSCEKIq|GVHQeuKV3o|dFXXtJTcp3|20KfwsrHIp|78AeNA1uyr|nejmcUiTNY|1EPuA6OM7q|yKXTRsO0Ma|ZNtj6R6Mbh|zXM0K4JPLH|RGEZY9b0fB|QjdAR3hDed|sPLX0kli9o|NXPI7qBq4n|s8wIq6rN9f|T7eBw9TWNW|    null|   null|    null|    null|     null|      DAA|    DCD|     null|      NWA|gZL4kT6j32|  CUST1|     null|Qm70IPpHuA|
+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+-------+--------+--------+---------+---------+-------+---------+---------+----------+-------+---------+----------+
only showing top 1 row

+---------+----------+----------+----------+--------+-------+--------+----------+--------+---------+----------+----------+----------+----------+--------+----------+-------+---------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+--------+---------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
|DOC_CATEG|DOC_NUMBER|S_ORD_ITEM|SALES_UNIT|DLV_STSO|DLV_STS|LW_GISTS|DSDEL_DATE|LST_A_GD|BMATL_BLK|   STS_ITM|  STS_BILL|   STS_PRC|   STS_DEL|DOC_TYPE| COMP_CODE|SOLD_TO|OBPARTNER| CUST_GRP1| CUST_GRP2|CUST_GRP3| CUST_GRP4| CUST_GRP5| DEL_BLOCK| SALES_OFF| SALES_GRP|  SALESORG|DISTR_CHAN|REASON_REJ|     CH_ON|     BATCH|BSHIP_MON|CALMONTH|CREATEDON| CREATEDBY| CREA_TIME|  STOR_LOC|MATL_GROUP|MATERIAL|MATL_GRP_1|MATL_GRP_2|MATL_GRP_3|MATL_GRP_4|MATL_GRP_5|BILLTOPRTY|     PAYER|   SHIP_TO| PROD_HIER| FORWAGENT|ITEM_CATEG|     ROUTE| SHIP_STCK|  DIVISION| STAT_DATE|   BND_IND| ST_UP_DTE| REFER_DOC| REFER_ITM|SHIP_POINT|     PLANT|   BADV_CD|  BBILL_AD| BILL_TYPE|  BBO_TYPE|  BBILL_ST|  BCAGE_CD|   BCNLDDT|     B_CAS|  BCONDITN|  BCONT_LN| BCNTRLNBR|     B_CRM|  BDISTRIB|   BDEMAND| BDMDPLANT|  BDOCIDCD|BDODAAC| BEXC_INFO|     B_SST|BFMS_PRG|  BFUND_CD|   BINVCNT|      BLOA|    BCLSSA| BMEDST_CD|  BMGMT_CD|     BMIPR|  BMIPRNBR|    BMODDT|  BORGDNUM|  BOWNRSHP| BPRIUSRID|  BPROJ_CD|    BPONBR|  BPURPOSE| BCONDCTRL| BRETNTQTY|  BRICFROM|  BSUFX_CD|  BSHIPNBR|   BSIG_CD| BSPLPGMRQ|  BSUPP_ST|  BSUPPADR| BILL_DATE|CUST_GROUP|PRICE_DATE|DOC_CATEGR| WBS_ELEMT| FISCVARNT|PROFIT_CTR|   CO_AREA|REJECTN_ST|    BAUDAT|    BKDMAT| BMFRPRTNO| BPROJ_NUM| BMFR_CAGE| BMFR_NAME|   DOCTYPE| BDISCP_CD| BMATRCVDT|  BMRA_QTY| BDISCPQTY|   BRIC_TO|      BMOQ| BHDRCRTDT|    BZZSSC|  BZZDMDDN|  BZZDMDSC| BZZODNPST|   BZZJOKO|  BZZUSECD| BZZCHRNCD| BZZPRIREF| BZZINSPCD| BZZPICKLS| BZZMATAQC| BZZRICSOS| BZZUTILCD| BGFN_CLIN| BGFM_CONT| BGFM_CALL| BMIPR_NBR|  BGFM_MDN|  BTPDDATE|   BTPDQTY|   BSTAGRP|
+---------+----------+----------+----------+--------+-------+--------+----------+--------+---------+----------+----------+----------+----------+--------+----------+-------+---------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+--------+---------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
|        E|    200000|      0010|sJef7aYIs1|    null|   null|    null|z1W1WPa5jo|    null|     null|lc9sGKbQm9|t4vBwumadr|0A5hPam6LP|gpIFuNwPon|    KIWI|3zR4agwCgr|  CUST1|    CUST1|pl7I8woq3h|Y3cBjITWMY|      NWA|0wM2LqPX7U|HVmMVeyDRo|StHqiniLxc|nxaZPg7htJ|Y5cVS9bgds|2zBArYK3T1|NQ1srApQUR|uK6NZknNjZ|7En2I8FRnV|4v9jvwUqyo|   201501|  201501| 20141010|vXQRonTVml|hMl3hxZPok|hmOUdl9GWD|opwVJjmwwu|ALUMINUM|yhtyUQmBjy|fGDkXd0Isr|NyhQJh7rFP|zNavRPRoyf|OfCZtAsbVt|gXkFeGUnxk|FiWWz9V6T8|qRqJ5bu503|Lk1PipZW2k|cebiF9KXnA|      ZTVC|HbellxvSs6|w842fqvL66|nRxQZy8LVE|mKyVNBTtcf|ALXFDGGCTk|jlbNc0NxoV|Ac4SCuEbeQ|lTo0h626uP|opqa42zowg|CB3qHf5u46|mXPegZAY0G|NWCM4SeHYU|KCoDj7VyxX|tS58eBgY9K|lr07ezd1To|GhElZgXgyd|mwiNSj1otK|sgvRdTQuQq|9dtNIuQRC9|96Yl6Ixl52|WaPqh0kpd6|D7MC0zzOHi|jZhRvjbgpi|mIDbn8vnpV|If2tQDfNJU|D1OkaS9gPh|    DCD|SaeWvVqyMq|j0hCGB5X9C|       A|LUGTKgaHCO|owXAB2N5wc|GoXJ6Z9Ruk|OV6yq0qjoO|X6DliLseS2|UVYTpgZ3ju|Qayb6lirtj|pWtNHZhPIJ|abgEQtVtOs|gZL4kT6j32|E8n08dPUIc|nOtnc0ubtG|lYcT9ZDl0r|4w3iK29JQH|ImpgiE0k05|Akkpzdc36M|VACfhBdyqp|bGpT6X45kf|kjpQGhVKTU|8gVs6d2uej|CGbaVfYuUg|Lwz0dv3iTj|cCVGNbOpu2|27YVhPTW5G|QIyTBY1Toy|qeFoLGGqEj|rcyPQfZSyk|dPB2vnkymM|7NsT0xEo0t|1J3lJXHCjv|pPMcy2Av02|S1kWVT0Cv3|FLy7qdH7oo|NvGwpt8DL3|nMHfcOc6W1|kJ5SqjrsGN|QzbHcduefF|ke51wRXcCI|2N30A3nqyn|zALzFGR06P|Q0fDmO2v0C|qnssrJpQRX|yhjKqB0fhp|NTx8j8GsaG|WrES5lETTj|rAZP8biv6L|qKXNBvgwpV|eO6o7qiX7M|w3tEhhOw9o|FJLOuVKJhC|IZn3ZhFJtL|YBMbpT1aWx|JoVYHVB2cQ|ktBZ5s1CIu|ZrLaQ2BdSU|Jxke0boO1N|KoJsjdFnw0|0UlCSNgtjl|lqUWg5AdXz|sZQowfQrts|yKXTRsO0Ma|ZNtj6R6Mbh|zXM0K4JPLH|NXPI7qBq4n|RGEZY9b0fB|s8wIq6rN9f|T7eBw9TWNW|Qm70IPpHuA|
+---------+----------+----------+----------+--------+-------+--------+----------+--------+---------+----------+----------+----------+----------+--------+----------+-------+---------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+--------+---------+----------+----------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-------+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
only showing top 1 row


ingestBeforeBofo20DF: org.apache.spark.sql.DataFrame = [DOC_NUMBER: string, S_ORD_ITEM: string ... 229 more fields]
+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+---------+--------+-------+--------+--------+---------+-------+--------+-------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+--------+----------+-----+-----+---------+--------+---------+---------+--------+----------+----------+----------+----------+----------+----------+----------+-----+-------+---------+---------+-----+---------+--------+---------+-------+---------+---------+---------+----------+-----+-------+--------+---------+--------+--------+--------+-------+-----+--------+--------+---------+-----+--------+-------+---------+--------+-------+---------+-----+--------+-------+----+------+---------+--------+-----+--------+------+--------+---------+--------+--------+---------+---------+--------+--------+--------+-------+---------+--------+--------+---------+----------+----------+----------+---------+---------+----------+-------+----------+------+------+---------+---------+---------+---------+-------+---------+---------+-------+----+---------+------+--------+--------+---------+-------+--------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+--------+--------+-------+-------+
|DOC_NUMBER|S_ORD_ITEM|BFMS_PRG|  BFMSCSCD|  BFMS_IND| BMILSVC_1| BMILSVC_2| BMILSVC_G| BMILSVC_S| BORDER_DT|MIN_DL_QTY|  BASE_UOM|  LOWR_BND|DOC_CURRCY|  BPURPRIC|   RTPLCST|CML_CF_QTY|  BTORDQTY| BTKILLQTY| BTKILLUOM|  BTVRSTCD|CREATEDON|  CONF_QTY| NET_VALUE|   REQ_QTY|   BCNDQTY|   BDMDQTY|   GIS_QTY|  BCONVERT| MATERIAL|      BIPG|SUBTOTAL_3|SUBTOTAL_4|SUBTOTAL_5|SUBTOTAL_6|SUBTOTAL_2|SUBTOTAL_1|  QCOASREQ| LOAD_DATE|SOLD_TO|   DLV_QTY|REQDEL_QTY|  BGR_DATE|RECORDMODE| BCUSTPODT|   BNS_RDD|BORD_TYPE|    BBSTNK|  BORGDNUM| BORGNLDOC|   BPRI_CD|CML_OR_QTY|DSDEL_DATE|      BMPC|ITEM_CATEG|REASON_REJ|LOC_CURRCY|  UPPR_BND|SALES_UNIT|    GR_QTY|  BSHIPTYP| BITMCATGP| BTSALEUNT|COND_PR_UN| NET_PRICE|    BPONBR|ACT_GI_DTE|DOC_TYPE|   PO_UNIT|      UNIT| PCONF_QTY|CML_CD_QTY|CONF_DATE| BCAGECDPN|    BIDNLF|    BMFRPN|  DLVQEYCR|  DLVQEYSC|  DLVQLECR| BMATRCVDT| BNMCS_IND|   BCASREP|  DLVQLESC| BCREATEDT|  DOC_DATE| B_YOBLOCK| B_ZTBLOCK|BOFBNSRDD| BOFITMCAT|  BOFPRICD|      BPLT| BACQADVCD|SCL_DELDAT|     B_ALT| DLV_BLOCK|  BMRA_QTY|CUST_GRP3|    BZZESD|  BEARCDTE|DOC_CATEG|DLV_STSO|DLV_STS|LW_GISTS|LST_A_GD|BMATL_BLK|STS_ITM|STS_BILL|STS_PRC|STS_DEL|COMP_CODE|OBPARTNER|CUST_GRP1|CUST_GRP2|CUST_GRP4|CUST_GRP5|DEL_BLOCK|SALES_OFF|SALES_GRP|SALESORG|DISTR_CHAN|CH_ON|BATCH|BSHIP_MON|CALMONTH|CREATEDBY|CREA_TIME|STOR_LOC|MATL_GROUP|MATL_GRP_1|MATL_GRP_2|MATL_GRP_3|MATL_GRP_4|MATL_GRP_5|BILLTOPRTY|PAYER|SHIP_TO|PROD_HIER|FORWAGENT|ROUTE|SHIP_STCK|DIVISION|STAT_DATE|BND_IND|ST_UP_DTE|REFER_DOC|REFER_ITM|SHIP_POINT|PLANT|BADV_CD|BBILL_AD|BILL_TYPE|BBO_TYPE|BBILL_ST|BCAGE_CD|BCNLDDT|B_CAS|BCONDITN|BCONT_LN|BCNTRLNBR|B_CRM|BDISTRIB|BDEMAND|BDMDPLANT|BDOCIDCD|BDODAAC|BEXC_INFO|B_SST|BFUND_CD|BINVCNT|BLOA|BCLSSA|BMEDST_CD|BMGMT_CD|BMIPR|BMIPRNBR|BMODDT|BOWNRSHP|BPRIUSRID|BPROJ_CD|BPURPOSE|BCONDCTRL|BRETNTQTY|BRICFROM|BSUFX_CD|BSHIPNBR|BSIG_CD|BSPLPGMRQ|BSUPP_ST|BSUPPADR|BILL_DATE|CUST_GROUP|PRICE_DATE|DOC_CATEGR|WBS_ELEMT|FISCVARNT|PROFIT_CTR|CO_AREA|REJECTN_ST|BAUDAT|BKDMAT|BMFRPRTNO|BPROJ_NUM|BMFR_CAGE|BMFR_NAME|DOCTYPE|BDISCP_CD|BDISCPQTY|BRIC_TO|BMOQ|BHDRCRTDT|BZZSSC|BZZDMDDN|BZZDMDSC|BZZODNPST|BZZJOKO|BZZUSECD|BZZCHRNCD|BZZPRIREF|BZZINSPCD|BZZPICKLS|BZZMATAQC|BZZRICSOS|BZZUTILCD|BGFN_CLIN|BGFM_CONT|BGFM_CALL|BMIPR_NBR|BGFM_MDN|BTPDDATE|BTPDQTY|BSTAGRP|
+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+---------+--------+-------+--------+--------+---------+-------+--------+-------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+--------+----------+-----+-----+---------+--------+---------+---------+--------+----------+----------+----------+----------+----------+----------+----------+-----+-------+---------+---------+-----+---------+--------+---------+-------+---------+---------+---------+----------+-----+-------+--------+---------+--------+--------+--------+-------+-----+--------+--------+---------+-----+--------+-------+---------+--------+-------+---------+-----+--------+-------+----+------+---------+--------+-----+--------+------+--------+---------+--------+--------+---------+---------+--------+--------+--------+-------+---------+--------+--------+---------+----------+----------+----------+---------+---------+----------+-------+----------+------+------+---------+---------+---------+---------+-------+---------+---------+-------+----+---------+------+--------+--------+---------+-------+--------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+--------+--------+-------+-------+
|    400000|      0040|       B|UZusM6cxlk|gtuc1cV7XC|uW1dHUBUwA|uapS7m4ynv|bpiyaCjkyd|iNC2aqmYT8|hU0ls08SYU|fLgVsgVF0P|trMQ4wIRVR|h2GDYnyW7O|MsFfotlnPj|Q59u870wew|GePUpzUUVh|x7YXH28Kzn|sIT0DWWDPU|Kswb3SuLP1|Y62B3Phset|SHuylOyvoL| 20161002|12Img1zmFc|9WCcfFVOIr|wV9iTHoi8S|ZFTsHIuU9j|d5u9Sp23Kq|LcXmtL91Qa|7PNp5c3bxu|STYROFOAM|DX9UIO8UPE|YLZVlUjc8R|NmFQUJBkGT|Nyde4ZwdpH|B1R5xi6z4w|dMXEVmvBPr|MLFLt4LXCj|ajEHKnlJfE|yTsdiGNqZj|  CUST4|FU6enwVoYa|B3PKu67u8O|D7N6rN0dn4|vWweepMixi|H7zmtxa19d|8jwChBauiI|        D|GVVay9nLDe|EVki2Pr5kx|IxvgW7BcBt|BI0QzMleYf|5TAcfv0Tt6|Ud7OCmJGGj|4R8qGEIvCN|      ZBDC|R8YDWl3aB7|EzaN4Aq8oK|7jAcSK0LTy|6vaTY8rIj5|PZxQHS8fTM|yyKsJRSLK5|F88CFf4gRN|WE6Gi9dk7j|TWoMZZM8Eb|a9iiBJUEYC|4r9rsOedGl|alMm97lK4R|    ZTVC|QZVOD2J8WV|SV1FP7Rz0o|kEzjQ4U5Vb|Lgo9mVN1mJ| 20151101|CfsSHeI5cm|H0kBcSx6pw|0NIWTo4RSK|PlbsG88Vj1|NIkynVixOO|DDhHr2Ibky|Ynx1skcKH3|eosKg6Jmvf|RWGu7BnwKM|JwfjNvzMqp|qTXJjDg5Ps|BeklcKxkgF|KMyNhfAo6j|T9iF2QIXJO| 20150801|IHlBWck0NN|x3l0d1Tx0l|ADopMtJr4p|pA3QE4w5cj|oAaXePgcdK|lER9dowXsc|AoNAt6JAwT|HXArMk6tLP|      NBN|BwKhuOBPmm|hYgMKUZ18s|     null|    null|   null|    null|    null|     null|   null|    null|   null|   null|     null|     null|     null|     null|     null|     null|     null|     null|     null|    null|      null| null| null|     null|    null|     null|     null|    null|      null|      null|      null|      null|      null|      null|      null| null|   null|     null|     null| null|     null|    null|     null|   null|     null|     null|     null|      null| null|   null|    null|     null|    null|    null|    null|   null| null|    null|    null|     null| null|    null|   null|     null|    null|   null|     null| null|    null|   null|null|  null|     null|    null| null|    null|  null|    null|     null|    null|    null|     null|     null|    null|    null|    null|   null|     null|    null|    null|     null|      null|      null|      null|     null|     null|      null|   null|      null|  null|  null|     null|     null|     null|     null|   null|     null|     null|   null|null|     null|  null|    null|    null|     null|   null|    null|     null|     null|     null|     null|     null|     null|     null|     null|     null|     null|     null|    null|    null|   null|   null|
+----------+----------+--------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+--------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+----------+----------+----------+----------+----------+----------+---------+----------+----------+---------+--------+-------+--------+--------+---------+-------+--------+-------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+--------+----------+-----+-----+---------+--------+---------+---------+--------+----------+----------+----------+----------+----------+----------+----------+-----+-------+---------+---------+-----+---------+--------+---------+-------+---------+---------+---------+----------+-----+-------+--------+---------+--------+--------+--------+-------+-----+--------+--------+---------+-----+--------+-------+---------+--------+-------+---------+-----+--------+-------+----+------+---------+--------+-----+--------+------+--------+---------+--------+--------+---------+---------+--------+--------+--------+-------+---------+--------+--------+---------+----------+----------+----------+---------+---------+----------+-------+----------+------+------+---------+---------+---------+---------+-------+---------+---------+-------+----+---------+------+--------+--------+---------+-------+--------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+--------+--------+-------+-------+
only showing top 1 row

*/
// Function to calculate lag date
def lagDate: String => String = (inDate: String) => {
  try {
    val zyr = Integer.parseInt(inDate.substring(0, 4))
    val zmth = Integer.parseInt(inDate.substring(4, 6))
    val zmthAdd = zmth + 2
    val zmthFinal = if (zmthAdd > 12) ("0" + (zmthAdd - 12)).substring(0, 2) else ("0" + zmth).substring(0, 2)
    val zyrFinal = if (zmthAdd > 12) zyr + 1 else zyr

    zyrFinal + zmthFinal + "00"
  } catch {
    case _: Throwable => null
  }
}

val lagDateUDF = udf[String, String](lagDate)

def selectBAVAILIND(bofo20DF: DataFrame): Try[DataFrame] = Try {
  val reasonList = List("R2", "R3", "R5", "R7", "R8")
  val reasonList2 = List("R4", "R6", "AA", "AR", "IR")
  val reasonList3 = List("CB", "Z3", "Z5", "BN", "OA", "ZJ", "ZF", "ZS", "MB", "MC", "FI")
  val reasonList4 = List("TAN", "TANN", "ZBDF", "ZBDP", "ZBDQ", "ZBDU", "ZKIT", "ZPRS", "ZRDD", "ZRFC", "ZRTN", "ZSA5", "ZSAF", "ZSDD", "ZSPP", "ZTAC")
  val sysDate = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())

  bofo20DF
    .withColumn("LAG_DATE", lagDateUDF(col("BTGT_SHIP")))
    .withColumn("ZQUANTITY", when(col("GR_QTY") > lit(0), col("GR_QTY")).otherwise(col("BSHIP_QTY")))
    .withColumn(
      "BAVAILIND",
      when(
        col("DOC_TYPE") === lit("ZTVC") || col("DOC_TYPE") === lit("ZCAT"),
        when(col("ITEM_CATEG") === lit("ZCAT"), lit("Y"))
          .otherwise(
            when(col("REASON_REJ") isin (reasonList), lit("R"))
              .otherwise(
                when(col("CONF_QTY") === col("BTKILLQTY") && col("REASON_REJ") === lit("AK"), lit("R"))
                  .otherwise(
                    when(col("BACT_SHIP") > col("BTGT_SHIP"), lit("N"))
                      .otherwise(
                        when(col("REASON_REJ") isin (reasonList2), lit("N"))
                          .otherwise(
                            when(
                              (length(col("BACT_SHIP")) > lit(0)) &&
                                (col("BACT_SHIP") <= col("BTGT_SHIP")) &&
                                (col("CONF_QTY") <= col("GR_QTY")),
                              lit("Y")
                            )
                              .otherwise(
                                when(
                                  (col("ITEM_CATEG") === lit("ZTVR")) &&
                                    (lit(sysDate) >= col("BTGT_SHIP")) &&
                                    (length(col("BCREATEDT")) < lit(1)),
                                  lit("N")
                                )
                                  .otherwise(
                                    when(
                                      (length(col("BACT_SHIP")) < lit(1)) &&
                                        (lit(sysDate) <= col("LAG_DATE")),
                                      lit("L")
                                    )
                                      .otherwise(
                                        when(
                                          (length(col("BACT_SHIP")) > lit(0)) &&
                                            (col("BACT_SHIP") <= col("BTGT_SHIP")) &&
                                            (lit(sysDate) <= col("LAG_DATE")) &&
                                            (col("CONF_QTY") > col("GR_QTY")),
                                          lit("L")
                                        ).otherwise(lit("N"))
                                      )
                                  )
                              )
                          )
                      )
                  )
              )
          )
      ).otherwise(
        when(col("REASON_REJ") === lit("CB"), lit("N"))
          .otherwise(
            when(length(col("BTGT_SHIP")) < lit(1), " ")
              .otherwise(
                when(col("ITEM_CATEG") === lit("ZTXT"), " ")
                  .otherwise(
                    when(
                      (col("ITEM_CATEG") === lit("ZSTO")) &&
                        (col("GR_QTY") <= lit(0)) &&
                        (length(col("DOC_DATE")) < 1) &&
                        (lit(sysDate) > col("LAG_DATE")),
                      lit("R")
                    )
                      .otherwise(
                        when(
                          (col("REASON_REJ") > lit(" ")) &&
                            ((lit(sysDate) <= col("LAG_DATE")) || (col("REASON_REJ") isin (reasonList3))),
                          lit("R")
                        )
                          .otherwise(
                            when(
                              (length(col("BACT_SHIP")) > lit(0)) &&
                                (col("BACT_SHIP") <= col("BTGT_SHIP")) &&
                                ((col("CONF_QTY") <= col("ZQUANTITY")) ||
                                  ((col("LOWR_BND") > lit(0)) && (col("REASON_REJ") isin (reasonList4)))),
                              lit("Y")
                            )
                              .otherwise(
                                when(col("BACT_SHIP") > col("BTGT_SHIP"), lit("N"))
                                  .otherwise(
                                    when(
                                      (lit(sysDate) >= col("BTGT_SHIP")) &&
                                        (length(col("DOC_DATE")) < lit(1)) &&
                                        (length(col("BCREATEDT")) < lit(1)),
                                      lit("N")
                                    )
                                      .otherwise(
                                        when(
                                          (length(col("BACT_SHIP") < lit(1))) &&
                                            (lit(sysDate) <= col("LAG_DATE")),
                                          lit("L")
                                        )
                                          .otherwise(
                                            when(
                                              (length(col("BACT_SHIP")) > lit(0)) &&
                                                (col("BACT_SHIP") <= col("BTGT_SHIP")) &&
                                                (lit(sysDate) <= col("LAG_DATE")) &&
                                                (col("CONF_QTY") > col("GR_QTY")),
                                              lit("L")
                                            ).otherwise(lit("N"))
                                          )
                                      )
                                  )
                              )
                          )
                      )
                  )
              )
          )
      )
    )
}

selectBAVAILIND(ingestBeforeBofo20DF).get.show(1)

// Example DataFrames and helper functions
val BCNFMILSPDF = Seq(
  ("0010", "A", "A", "A", "B", "A", "*", "Y", "A", "A", "AAD", "DDA", "AAAAAAAAAA"),
  ("0020", "B", "*", "*", "*", "*", "*", "Y", "A", "B", "DAA", "ACC", "BBBBBBBBBB")
).toDF("BCNFMILSP", "OBJVERS", "CHANGED", "BSERVICE1", "BSERVICE2", "BSERVICE3", "BFMS_IND", "BMILGP1FG", "BMILGP2FG", "BMILGP3FG", "BMILGP1RS", "BMILGP2RS", "BMILGP3RS")

val ZPCUSTOMERDF = Seq(
  ("EJnb9IccRI", "A", "AAA"),
  ("0et2E5sTBM", "B", "ABB")
).toDF("CUSTOMER", "OBJVERS", "CUSTGRP3")

def dateParseUdf = udf((rawDate: String) => {
  try {
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val c = new GregorianCalendar()
    c.setLenient(false)
    c.set(rawDate.substring(0, 4).toInt, rawDate.substring(4, 6).toInt - 1, rawDate.substring(6, 8).toInt)
    formatter.format(c.getTime)
  } catch {
    case _: Throwable => null
  }
})

val addDays = udf((rawDate: String, day2Add: Int) => {
  try {
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val c = new GregorianCalendar()
    c.setLenient(false)
    c.set(rawDate.substring(0, 4).toInt, rawDate.substring(4, 6).toInt - 1, rawDate.substring(6, 8).toInt)
    c.add(Calendar.DATE, day2Add)
    formatter.format(c.getTime)
  } catch {
    case _: Throwable => null
  }
})

val addDaysUdf = udf[String, String](addDays)
val dateTestDF = Seq(("20160101", 44),("20160202", 33),("20160606", 22),("20160202", 11)).toDF("dateTest", "addDays")

dateTestDF.select($"dateTest", $"addDays", addDays(col("dateTest"), lit(2))).show()

def julianDateParse(myJulianYDDD: String) = {       
    
    try
    {
        val sysDate: String =  new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())
        val sysDateDecade: String = sysDate.substring(0,3)
        if (sysDateDecade + myJulianYDDD > sysDate) { (Integer.parseInt(sysDateDecade) - 1) + myJulianYDDD }  else sysDateDecade + myJulianYDDD
        
    }
    catch
    {
        case e: Throwable => null
    }

} //end dateParse2
}
