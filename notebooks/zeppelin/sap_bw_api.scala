import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import scala.util.Try
import spark.implicits._
import org.apache.hadoop.fs.FileSystem
import sys.process._
import org.joda.time.format._
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import java.util.Calendar

def getCurrentTime = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance().getTime())
    }

def getTimeDifference(startTime: String) = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    val date1: java.util.Date = format.parse(startTime);
    val date2 = format.parse(getCurrentTime);
    val difference = date2.getTime() - date1.getTime();
    "Running time - " + (difference / 1000 / 60) + ":" + ("0" + ((difference * .001) % 60 ).toInt).takeRight(2) + "." + (difference % 1000) 
    }
    
val bofo20Start = getCurrentTime

/* Object IngestAndWrite
*
*  This object will encapsulate the logic for reading and writing the HDFS
*  
*  Methods - loadSchema, loadEntity, writeEntity
*
*  Functions - schemaMapper
*/

object IngestAndWrite {
   /*    
   * Method - LoadSchema
   *    
   * Parameters - inKeys:String,    
   *              inDateFields:String, 
   *              inIntFields:String, 
   *              inTimestampFields:String,
   *              inDecimalFields:String,ß
   *              inAllFieldsInOrder:String
   *    
   * Return Type -Tuple6(  Keys:List[String],
   *     DateFields:List[String],
   *     IntFields:List[String], 
   *     TimestampFields:List[String],
   *     DecimalFieldsList[String],
   *     AllFieldsInOrderList[String]
   *     )    
   *    
   * This method will read in a comma delimeted string for the key fields, the date fields, integer fields, timestamp fields, decimal fields, and all the fields
   * If there are other data types found after bofo08 then we need to add those as well if they arent already covered.  
   * For example, Strings cover 'CHAR' and 'UNIT' so we don't need a second data type.
   * Also, the string data type is the most comon so that is the default and does not need to be specified. 
   */    
   
    def loadSchema(inKeys: String, 
        inDateFields: String, 
        inIntFields: String, 
        inTimestampFields: String, 
        inDecimalFields: String
    )(inAllFieldsInOrder: String): (
        List[String], 
        List[String], 
        List[String], 
        List[String], 
        List[String], 
        List[String]) = {
        (inKeys.split(",").toList, 
        inDateFields.split(",").toList, 
        inIntFields.split(",").toList, 
        inTimestampFields.split(",").toList, 
        inDecimalFields.split(",").toList, 
        inAllFieldsInOrder.split(",").toList)}
    /*
    * Function - schemaMapper
    * Parameters - fieldName:String
    *              isDeltaQueue: Boolean,
    *              keys:List[String],
    *              dateFields:List[String],
    *              intFields:List[String],
    *              timestampFields:List[String],
    *              decimalFieldsList[String]
    *
    * Return Type - schema:StructField
    *
    * Used to map the field name to the proper data type based on the the user provided strings.
    * This function is designed to use the output of loadSchema as the input.
    * The function will be used to interate through a the list of fieldLists
    */
         
    def schemaMapper : (
        String, 
        List[String], 
        List[String], 
        List[String], 
        List[String], 
        List[String]) => StructField = (
        fieldName: String, 
        keys: List[String], 
        dateFields : List[String], 
        intFields : List[String], 
        timestampFields : List[String], 
        decimalFields : List[String]) => fieldName match {
            case x if keys.contains(x) && intFields.contains(x) => StructField(fieldName, IntegerType, nullable =  false)
            case x if keys.contains(x) => StructField(fieldName, StringType, nullable =  true)
            case x if intFields.contains(x) => StructField(fieldName, IntegerType, nullable =  true)
            case _ => StructField(fieldName, StringType, nullable =  false)
        }
                
        /*
        * Method - loadEntity
        * Parameters - schema:StructType, fullyQualifiedPath:String, formatType: StringType
        * 
        * Return Type - Try[DataFrame]
        *
        * This method takes in a string and a schema.  The schema will be from the manually create comma delimeted strings passed to loadschema.  
        * Then Lists of strings are used to create the schema.
        * The path of the load file is hardcoded for now but can be added as parameter
        */
        
    def loadEntity(schema: StructType, fullyQualifiedPath: String, formatType: String): Try[DataFrame] = Try({
        spark.read.format(formatType)
            .option("delimiter","\t")
            .option("header", "false")
            .schema(schema)
            .load(fullyQualifiedPath)
            })//loadEntity end
                
        /*
        * Method - writeEntity
        * Parameters -outputDF: DataFrame, path: String, writeMode: String, formatType: StringType
        *
        * Return Type - Unit 
        *
        * Writes the dataframe to path given in HDFS, using the mode 'writeMode'
        * _SUCCESS file that is created will be deleted 
        *
        * This write to different formats
        */
        
        def writeEntity(outputDF: DataFrame, path: String, writeMode: String, formatType: String): Unit =  {
            outputDF.write
                .mode(writeMode)
                .option("delimiter","\t")
                .option("header", "false")
                .format(formatType).save(path)
                
            val fs:FileSystem = FileSystem.get(new java.net.URI(path + "/_SUCCESS"), sc.hadoopConfiguration)

            fs.delete(new org.apache.hadoop.fs.Path(path + "/_SUCCESS"), false)
            }//writeEntity end
            
        def dynamicPartitionNumber(multiplierInt: Int) = {
            val cores = spark.sparkContext.getConf.get("spark.executor.cores").toInt
            val allExecutors = spark.sparkContext.getExecutorMemoryStatus
            val driver = spark.sparkContext.getConf.get("spark.driver.host")
            allExecutors.filter(! _._1.split(":")(0).equals(driver)).toList.length * multiplierInt * cores
            }}//end  IngestAndWrite
            
/* Object ProcessBOF
*
*  This object will encapsulate the logic for processing the BOF.  The methods for each BOF will be different
* 
*  Methods - updateTran01, updateTran01AndSaveErrorDF, joinBof08AndTran01
*/
object ProcessBOF extends Serializable {

    /*
     * Method - unallowable
     * Parameters - rawString: String
     * 
     * Return Type - Boolean
     *
     * This method checks the string column values for any unallowable character defined in the list.
     * If any unallowable character is found then the method will return True else returns False.
     */
     
    def unallowable: String => Boolean = (rawString: String) => {
    
        //Unallowable character list
        //$!@#%^&*()_+=-{}[]|\\:";'<>?,./~`
        val list = List("$","!","@","#","%","^","&","*","(",")","_","+","=","-","{","}","[","]","|","""\""",":",""""""",";","'","<",">","?",",",".","/","~","`")
        if (rawString == null) {
            false
        } else {
            list.exists(rawString.contains)
        }}
        
        val unallowableUDF = udf[Boolean, String](unallowable)

    /*
    * Method - updateDeltaQueue
    * Parameters - deltaQueueDF: DataFrame
    *
    * Return Type - Try[DataFrame]
    *
    * This method updates the delata queue dataframe based on the transformation logic.
    * The transformed dataframe is returned.
    * This needs to change completely per BOF and DQ
    */
        
    def updateDeltaQueue(deltaQueueDF: DataFrame): Try[DataFrame] = Try({
        deltaQueueDF})

    /*
    * Method - massageDeltaQueueAndReturnErrorDF
    * Parameters - deltaQueueDF: DataFrame, inDates: String, badCharacterCols: String
    * Return Type - Try[Tuple2(DataFrame, DataFrame)]
    *
    * This method updates the delta queue and also creates the error dataframe.
    * All the fields for bad character checking and dates can be passed in as two string arguemnts and they will be handled dynamically
    * 1. DataFrame `allColumnDF` selects all columns from incoming deltaQueue DataFrame and applies functions and return new columns and old.
    * 2. DataFrame `massagedDF`  selects only the columns that will be used for downstream processing. Dates of 'string' datatype and ErrorChecks will be left behind.
    * 3. DataFrame `errorDF`  selects all columns including newly created error columns and old raw columns.
    * A tuple is returned with the updated delta queue and the error dataframe.
    */
        
        spark.udf.register("unallowableUDF", unallowable)
        
    def massageDeltaQueueAndReturnErrorDF(
        deltaQueueDF: DataFrame, 
        inDates: String, 
        badCharacterCols: String): Try[(DataFrame, Option[DataFrame])] = Try({
            (deltaQueueDF, None)
            })
            
    /*
    * Method - joinBofAndDeltaQueue
    * Parameters - deltaQueueDF: DataFrame, bofDF: DataFrame, keys: Array[String]
    * 
    * Return Type - Try[DataFrame]
    *
    * This method merges the bof and the delta queue (tran01DF), the assumption is that the keys to join are the same name of columns
    * A dataframe is returned that is the intersect of bof:DataFrame and tran01DF:DataFrame unioned with the set difference between bof:DataFrame and tran01DF:DataFrame
    */
    
    def joinBofAndDeltaQueue(deltaQueueDF: DataFrame, bofDF: DataFrame, keys: Array[String]): Try[DataFrame] = Try({
        val joinExprs = keys
            .map{ case (c) => deltaQueueDF(c) <=> bofDF(c) }
            .reduce(_ && _)
            
        bofDF.join(deltaQueueDF, joinExprs, "outer")
            .select(bofDF.columns.map(c => if (!(deltaQueueDF.columns contains "$c")) 
            
        bofDF.col(s"$c") else coalesce(deltaQueueDF.col(s"$c"), bofDF.col(s"$c")) as c): _*)
        })}//ProcessBOF
        
    /* Object Zarixsd2
    *
    *  This object will encapsulate the logic for processing the archive index.  The keys per BOF will be different, need to think about a way to pass thme dynamically
    *  
    *  Methods - loadZarixsd2, mergeBofWithZarixsd2
    */
    
    object Zarixsd2 {
        /*
        * Method - loadZarixsd2
        *
        * Parameters - path: String
        *
        * Return Type - Try[DataFrame]
        *
        * This will load the archive table given the strings above in the Zarixsd2 object
        * Uses the IngestAndWrite methods loadSchema and the function schemaMapper
        */
        
        def loadZarixsd2(path: String) : Try[DataFrame] = {
            /*
            * These fields below cann be hard coded because there is only one Zarixsd2.
            * More dataytpes need to be handled though
            */
            val keysString = "DOC_NUMBER,S_ORD_ITEM"
            val datesString = "BEARCDTE"
            val integersString = ""
            val timestampsString = ""
            val decimalsString = ""
            val allFieldsString = "COL_NAME,REQUEST,DATAPAKID,PARTNO,RECORD,DOC_NUMBER,S_ORD_ITEM,BARCHKEY,BARCHOFST,SOLD_TO,BBSTNK,MATERIAL,DOC_TYPE,BAUDAT,SALESORG,BORGDNUM,BEARCDTE,RECORDMODE"
            val fieldLists = IngestAndWrite.loadSchema(keysString,datesString,integersString,timestampsString,decimalsString)(allFieldsString)
        
            IngestAndWrite.loadEntity( { 
                StructType(fieldLists._6.map(x => 
                    IngestAndWrite.schemaMapper(x, fieldLists._1, fieldLists._2, fieldLists._3, fieldLists._4, fieldLists._5)))}, path, "csv")}
                    
        
        /*
        * Method - mergeBofWithZarixsd2
        *
        * Parameters - bofDF: DataFrame, zarixsd2DF: DataFrame, orderedColumnsString: String, keys: Array[String]
        *
        * Return Type - Try[DataFrame]
        * This will pull in Z2 archive table and merge with a the respective BOF. BOF and Zarixsd2 are arguments
        * The BEARCDTE column coming from the bof will be kept if not null and if it is Null in bof, the BEARCDTE column in
        * zarixsd2 will be used.  So, the bof will need to have nulls, and not blanks if it will work.  If the bof will have something 
        * other than nulls, then this function needs to be changed accordingly
        */
    
        def mergeBofWithZarixsd2(
            bofDF: DataFrame, 
            zarixsd2DF: DataFrame, 
            orderedColumnsString: String, 
            keys: Array[String]): Try[DataFrame] = Try({
            
            import spark.implicits._
            val joinExprs = keys.map{ case (c) => zarixsd2DF(c) <=> bofDF(c)}
                .reduce(_ && _)
                
            val orderedColumns = orderedColumnsString.split(",").toSeq
            
            zarixsd2DF.join(bofDF, joinExprs ,"rightouter")
                .select(bofDF.columns.map(c => if (s"$c" == "BEARCDTE") coalesce(zarixsd2DF.col(s"$c"), bofDF.col(s"$c")).alias("BEARCDTE") else bofDF.col(s"$c") as c): _*)
                .select(orderedColumns.head, orderedColumns.tail: _*)
                })
                
         /*This will check for something to do the archive load. The mechanism is TBD */
         
         def triggerZarixsd2Load() : Boolean = true