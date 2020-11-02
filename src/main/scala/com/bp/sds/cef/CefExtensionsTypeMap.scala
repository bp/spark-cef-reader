package com.bp.sds.cef

import org.apache.spark.sql.types._

import scala.collection.immutable
import scala.collection.immutable.HashMap

/**
 * Provides functions and properties for known CEF extensions
 */
object CefExtensionsTypeMap {
  /**
   * Known extensions and their data type mapping
   */
  val extensions: HashMap[String, StructField] = immutable.HashMap(
    "cfp1" -> StructField("cfp1", FloatType, nullable = true), /* deviceCustomFloatingPoint1 */
    "cfp2" -> StructField("cfp2", FloatType, nullable = true), /* deviceCustomFloatingPoint2 */
    "cfp3" -> StructField("cfp3", FloatType, nullable = true), /* deviceCustomFloatingPoint3 */
    "cfp4" -> StructField("cfp4", FloatType, nullable = true), /* deviceCustomFloatingPoint4 */
    "cn1" -> StructField("cn1", LongType, nullable = true), /* deviceCustomNumber1 */
    "cn2" -> StructField("cn2", LongType, nullable = true), /* deviceCustomNumber2 */
    "cn3" -> StructField("cn3", LongType, nullable = true), /* deviceCustomNumber3 */
    "cnt" -> StructField("cnt", LongType, nullable = true), /* baseEventCount */
    "destinationTranslatedPort" -> StructField("destinationTranslatedPort", IntegerType, nullable = true), /* destinationTranslatedPort */
    "deviceCustomDate1" -> StructField("deviceCustomDate1", TimestampType, nullable = true), /* deviceCustomDate1 */
    "deviceCustomDate2" -> StructField("deviceCustomDate2", TimestampType, nullable = true), /* deviceCustomDate2 */
    "deviceDirection" -> StructField("deviceDirection", IntegerType, nullable = true), /* deviceDirection */
    "dpid" -> StructField("dpid", IntegerType, nullable = true), /* destinationProcessId */
    "dpt" -> StructField("dpt", IntegerType, nullable = true), /* destinationPort */
    "end" -> StructField("endTime", TimestampType, nullable = true), /* endTime */
    "dvcpid" -> StructField("deviceProcessId", IntegerType, nullable = true), /* deviceProcessId */
    "fileCreateTime" -> StructField("fileCreateTime", TimestampType, nullable = true), /* fileCreateTime */
    "fileModificationTime" -> StructField("fileModificationTime", TimestampType, nullable = true), /* fileModificationTime */
    "flexDate1" -> StructField("flexDate1", TimestampType, nullable = true), /* flexDate1 */
    "fsize" -> StructField("fsize", TimestampType, nullable = true), /* fileSize */
    "in" -> StructField("in", IntegerType, nullable = true), /* bytesIn */
    "oldFileCreateTime" -> StructField("oldFileCreateTime", TimestampType, nullable = true), /* oldFileCreateTime */
    "oldFileModificationTime" -> StructField("oldFileModificationTime", TimestampType, nullable = true), /* oldFileModificationTime */
    "oldFileSize" -> StructField("oldFileSize", IntegerType, nullable = true), /* oldFileSize */
    "out" -> StructField("out", IntegerType, nullable = true), /* bytesOut */
    "rt" -> StructField("rt", TimestampType, nullable = true), /* deviceReceiptTime */
    "sourceTranslatedPort" -> StructField("sourceTranslatedPort", IntegerType, nullable = true), /* sourceTranslatedPort */
    "spid" -> StructField("spid", IntegerType, nullable = true), /* sourceProcessId */
    "spt" -> StructField("spt", IntegerType, nullable = true), /* sourcePort */
    "start" -> StructField("start", TimestampType, nullable = true), /* startTime */
    "type" -> StructField("type", IntegerType, nullable = true), /* type */
    "art" -> StructField("art", TimestampType, nullable = true), /* agentReceiptTime */
    "dlat" -> StructField("dlat", DoubleType, nullable = true), /* destinationGeoLatitude */
    "dlong" -> StructField("dlong", DoubleType, nullable = true), /* destinationGeoLongitude */
    "eventId" -> StructField("eventId", LongType, nullable = true), /* eventId */
    "slat" -> StructField("slat", DoubleType, nullable = true), /* sourceGeoLatitude */
    "slong" -> StructField("slong", DoubleType, nullable = true), /* sourceGeoLongitude */
  )
}
