package com.bp.sds.cef

import java.io.{BufferedReader, InputStream, InputStreamReader, SequenceInputStream}
import java.sql.Timestamp
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField.{OFFSET_SECONDS, YEAR}
import java.time.{Instant, LocalDateTime, ZoneOffset, ZonedDateTime}

import com.bp.sds.cef.CefRecordParser.replaceLineCharsForNull
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, DateTimeUtils, PermissiveMode}
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.util.Try
import scala.util.matching.Regex

/**
 * Implements a parser for reading CEF records
 *
 * @param options a set of options defining how parsing should operate
 */
private[cef] class CefRecordParser(options: CefParserOptions) extends Logging {
  /**
   * Cleans a CEF record string
   *
   * @param value the CEF record to clean
   * @return the CEF record with common cleaning operations applied
   */
  def cleanString(value: String): String = value.replaceAll("\\\\\\\\", "\\\\")

  /**
   * The regular expression required for extracting CEF record extension data
   */
  val extensionPattern: Regex = """(?m)([\w._-]+)=(.+?)(?=$|\s[\w._-]+=)""".r

  /**
   * Takes a CEF record and transforms it to a row of data
   *
   * @param cef            the CEF record to parse
   * @param requiredSchema the schema which the output needs to adhere to
   * @return a row of data which matches the required schema
   */
  def parseToInternalRow(cef: String, requiredSchema: StructType): InternalRow = {
    try {
      val cefToProcess = replaceLineCharsForNull(cef, options)
      val hashRecord = parse(cefToProcess, requiredSchema.fields)
      val rowData = requiredSchema.fields.map(f => hashRecord(f.name))
      InternalRow.fromSeq(rowData)
    } catch {
      case e: CefRecordParserException =>
        val recordString = UTF8String.fromString(cef)
        e.partialData match {
          case Some(data) =>
            val resultRow = new GenericInternalRow(requiredSchema.fields.length)
            requiredSchema.fields.foreach { f =>
              if (f.name == options.corruptColumnName) {
                resultRow(requiredSchema.fieldIndex(f.name)) = recordString
              } else {
                val fieldValue = data.get(f.name)
                if (fieldValue.nonEmpty && fieldValue.get != null) {
                  resultRow(requiredSchema.fieldIndex(f.name)) = fieldValue.get
                }
              }
            }
            throw BadRecordException(() => recordString, () => Some(resultRow), e)
          case None => throw BadRecordException(() => recordString, () => None, e)
        }
    }
  }

  /**
   * Parses a CEF record string into a map of values
   *
   * @param cef    the CEF record to parse
   * @param fields the required output schema
   * @return a map of key value pairs parsed from the CEF record
   */
  def parse(cef: String, fields: Array[StructField]): mutable.AnyRefMap[String, Any] = {
    // Initial the buffer with twice the size we need to reduce resize requests
    val rowData = new mutable.AnyRefMap[String, Any](fields.length * 2)

    fields.foreach(f =>
      rowData += (f.name -> null)
    )

    val cefSplit = cef.split("""(?<!\\)\|""", 8)
    rowData("CEFVersion") = UTF8String.fromString(cefSplit(0))

    try {
      rowData("DeviceVendor") = UTF8String.fromString(cefSplit(1))
    } catch {
      case e: Exception => throw CefRecordParserException("Missing device vendor in record", Some(rowData), e)
    }
    try {
      rowData("DeviceProduct") = UTF8String.fromString(cefSplit(2))
    } catch {
      case e: Exception => throw CefRecordParserException("Missing device product in record", Some(rowData), e)
    }
    try {
      rowData("DeviceVersion") = UTF8String.fromString(cefSplit(3))
    } catch {
      case e: Exception => throw CefRecordParserException("Missing device version in record", Some(rowData), e)
    }
    try {
      rowData("SignatureID") = UTF8String.fromString(cefSplit(4))
    } catch {
      case e: Exception => throw CefRecordParserException("Missing signature id in record", Some(rowData), e)
    }
    try {
      rowData("Name") = UTF8String.fromString(cefSplit(5))
    } catch {
      case e: Exception => throw CefRecordParserException("Missing name in record", Some(rowData), e)
    }
    try {
      rowData("Severity") = UTF8String.fromString(cefSplit(6))
    } catch {
      case e: Exception => throw CefRecordParserException("Missing severity in record", Some(rowData), e)
    }

    if (cefSplit.length != 8) {
      throw new CefRecordParserException("Record does not contain the correct number of pipe separated values", rowData)
    }

    // Construct a new map based on the required field output, this may require resize requests as the required output
    // might not match the full data set size, but this should reduce the number of resize requests required to complete
    // the operation
    val map = new mutable.AnyRefMap[String, Any](fields.length * 2)
    buildHashmapFromCEF(cefSplit(7), map)

    val mandatoryFieldKeys = CefRecordParser.mandatoryFields.keys.map(_.toLowerCase)

    if (options.pivotFields) {
      val pivotFields = map.filter(m => map.contains(s"${m._1}Label"))
      pivotFields.foreach(pf => {
        val label = map(s"${pf._1}Label").toString
        val checkedLabel = if (mandatoryFieldKeys.exists(_ == label.toLowerCase)) s"${label}_1" else label
        map.put(checkedLabel, pf._2)
        map.remove(s"${pf._1}Label")
        map.remove(pf._1)
      })
    }

    map.foreach(item => {
      // Modify the key if the field is a value which conflicts with the mandatory fields
      val mapLabel = if (mandatoryFieldKeys.exists(_ == item._1.toLowerCase)) s"${item._1}_1" else item._1
      if (rowData.contains(mapLabel)) {
        try {
          val dt = fields.filter(_.name == mapLabel).head.dataType
          val itemValue = dt match {
            case IntegerType => item._2.asInstanceOf[java.lang.Integer]
            case LongType => item._2.asInstanceOf[java.lang.Long]
            case FloatType => item._2.asInstanceOf[java.lang.Float]
            case DoubleType => item._2.asInstanceOf[java.lang.Double]
            case TimestampType => if (item._2 == null) null else DateTimeUtils.fromJavaTimestamp(item._2.asInstanceOf[Timestamp])
            case _ => UTF8String.fromString(item._2.asInstanceOf[String])
          }
          if (rowData.contains(mapLabel)) rowData(mapLabel) = itemValue
        } catch {
          case _: Exception => throw new CefRecordParserException("An error occurred parsing the record.", rowData)
        }
      }
    })

    rowData
  }

  /**
   *
   * @param cef     the CEF record to parse
   * @param hashMap a [[scala.collection.mutable.HashMap]] to update with the parsed values
   * @return a collection of key-value pairs as a map
   */
  private def buildHashmapFromCEF(cef: String, hashMap: mutable.AnyRefMap[String, Any]): Unit = {
    val cleanedCef = cleanString(cef)
    extensionPattern.findAllIn(cleanedCef).matchData.foreach(m => {
      val key = m.group(1)
      var baseValue = m.group(2).trim
      val mapItem = CefExtensionsTypeMap.extensions.get(key)
      if (baseValue == options.nullValue) baseValue = null
      val value = if (baseValue == null) null else mapItem match {
        case Some(item) => item.dataType match {
          case IntegerType => CefRecordParser.tryParseInt(baseValue)
          case LongType => CefRecordParser.tryParseLong(baseValue)
          case FloatType => CefRecordParser.tryParseFloat(baseValue)
          case DoubleType => CefRecordParser.tryParseDouble(baseValue)
          case TimestampType => CefRecordParser.tryParseTimestamp(baseValue)
          case _ => baseValue
        }
        case None => baseValue
      }

      hashMap.put(key, value)
    })
  }
}

private[cef] object CefRecordParser extends Logging {

  /**
   * Get the current year to default timestamps without year identifiers
   */
  private val defaultYear = 1970

  private val mandatoryFields = mutable.LinkedHashMap[String, DataType](
    "CEFVersion" -> StringType,
    "DeviceVendor" -> StringType,
    "DeviceProduct" -> StringType,
    "DeviceVersion" -> StringType,
    "SignatureID" -> StringType,
    "Name" -> StringType,
    "Severity" -> StringType
  )

  /**
   * A collection of valid timestamp formats for CEF records
   */
  private val timestampFormats = Vector(
    DateTimeFormatter.ISO_DATE_TIME,
    DateTimeFormatter.ISO_OFFSET_DATE_TIME,
    DateTimeFormatter.ISO_LOCAL_DATE_TIME,
    DateTimeFormatter.ISO_ZONED_DATE_TIME,

    new DateTimeFormatterBuilder().appendPattern("MMM dd yyyy HH:mm:ss")
      .parseDefaulting(OFFSET_SECONDS, 0)
      .toFormatter(),

    new DateTimeFormatterBuilder().appendPattern("MMM dd yyyy HH:mm:ss.SSS zzz")
      .toFormatter(),

    new DateTimeFormatterBuilder().appendPattern("MMM dd yyyy HH:mm:ss.SSS")
      .parseDefaulting(OFFSET_SECONDS, 0)
      .toFormatter(),

    new DateTimeFormatterBuilder().appendPattern("MMM dd yyyy HH:mm:ss zzz")
      .toFormatter(),

    new DateTimeFormatterBuilder().appendPattern("MMM dd HH:mm:ss")
      .parseDefaulting(YEAR, defaultYear)
      .parseDefaulting(OFFSET_SECONDS, 0)
      .toFormatter(),

    new DateTimeFormatterBuilder().appendPattern("MMM dd HH:mm:ss.SSS zzz")
      .parseDefaulting(YEAR, defaultYear)
      .toFormatter(),

    new DateTimeFormatterBuilder().appendPattern("MMM dd HH:mm:ss.SSS")
      .parseDefaulting(YEAR, defaultYear)
      .parseDefaulting(OFFSET_SECONDS, 0)
      .toFormatter(),

    new DateTimeFormatterBuilder()
      .appendPattern("MMM dd HH:mm:ss zzz")
      .parseDefaulting(YEAR, defaultYear)
      .toFormatter()
  )

  /**
   * Attempts to parse a string to a [[java.lang.Long]] value
   *
   * @param value the string value to parse
   * @return an [[java.lang.Long]] if successful, otherwise null
   */
  private def tryParseLong(value: String): java.lang.Long = {
    try {
      value.toLong
    } catch {
      case _: Exception => null
    }
  }

  /**
   * Attempts to parse a string to a [[java.lang.Integer]] value
   *
   * @param value the string value to parse
   * @return an [[java.lang.Integer]] if successful, otherwise null
   */
  private def tryParseInt(value: String): java.lang.Integer = {
    try {
      value.toInt
    } catch {
      case _: Exception => null
    }
  }

  /**
   * Attempts to parse a string to a [[java.lang.Float]] value
   *
   * @param value the string value to parse
   * @return an [[java.lang.Float]] if successful, otherwise null
   */
  private def tryParseFloat(value: String): java.lang.Float = {
    try {
      value.toFloat
    } catch {
      case _: Exception => null
    }
  }

  /**
   * Attempts to parse a string to a [[java.lang.Double]] value
   *
   * @param value the string value to parse
   * @return an [[java.lang.Double]] if successful, otherwise null
   */
  private def tryParseDouble(value: String): java.lang.Double = {
    try {
      value.toDouble
    } catch {
      case _: Exception => null
    }
  }

  /**
   * Attempt to parse a string into a timestamp
   *
   * @param value the string value to parse
   * @return a Timestamp value
   */
  private def tryParseTimestamp(value: String): Timestamp = {
    val timestamp = tryParseLong(value) match {
      case ts if ts != null => Timestamp.from(Instant.ofEpochMilli(ts))
      case null =>
        val parsed = timestampFormats.flatMap(f => {
          val zt = Try(ZonedDateTime.parse(value, f))
          val lt = Try(LocalDateTime.parse(value, f))
          if (zt.isSuccess) Some(zt.get.toInstant)
          else if (lt.isSuccess) Some(lt.get.toInstant(ZoneOffset.UTC))
          else None
        })
        if (parsed.nonEmpty) Timestamp.from(parsed.head) else null
    }
    timestamp
  }

  /**
   * Get a [[java.io.InputStream]] reader object for a given path, handling compressed and decompressed data
   *
   * @param path the [[org.apache.hadoop.fs.Path]] object to read
   * @param conf a [[org.apache.hadoop.conf.Configuration]] object to provide HDFS configuration
   * @return an [[java.io.InputStream]] for reading the source file
   */
  def getReader(path: Path, conf: Configuration): InputStream = {
    CodecStreams.createInputStreamWithCloseResource(conf, path)
  }

  /**
   * Get the data type for a field by checking known CEF extensions
   *
   * @param name the name of the field
   * @return a [[org.apache.spark.sql.types.DataType]] for the field, defaulting to a string
   */
  private def getDataType(name: String): DataType = {
    CefExtensionsTypeMap.extensions.get(name) match {
      case Some(keyType) => keyType.dataType
      case None => StringType
    }
  }

  /**
   * Update a collection of fields based on a based on a record set, adding new fields if they do not exist in
   * the collection
   *
   * @param records the records to update the fields with
   * @param fields  an existing collection of fields
   */
  private def updateFields(records: mutable.AnyRefMap[String, Any], fields: mutable.LinkedHashMap[String, DataType]): Unit = {
    val mandatoryFieldKeys = mandatoryFields.keys.map(_.toLowerCase)
    records.foreach(r => {
      val label = if (mandatoryFieldKeys.exists(_ == r._1.toLowerCase)) s"${r._1}_1" else r._1
      if (!fields.contains(label)) {
        val dataType = getDataType(r._1)
        fields += (label -> dataType)
      }
    })
  }

  /**
   * Infers the schema of the data from a set of files, it will continue to read through the files until the max
   * number of records defined in the [[CefParserOptions]] is reached, or there is no more data to read
   *
   * @param path       a collection of [[org.apache.hadoop.fs.FileStatus]] objects to infer the schema from
   * @param hadoopConf the [[org.apache.hadoop.conf.Configuration]] used for accessing the source data
   * @param options    a set of options used for defining how the inference should operate
   * @return a [[org.apache.spark.sql.types.StructType]] of fields representing the data schema
   */
  def inferSchema(path: Seq[FileStatus], hadoopConf: Configuration, options: CefParserOptions): StructType = {
    val parser = new CefRecordParser(options)

    val maxRecords = options.maxRecords match {
      case mr if mr >= 1 => mr
      case _ => 10000
    }

    logInfo(s"Inferring schema based on $maxRecords rows")

    val fields = mandatoryFields.clone()
    val mandatoryFieldKeys = mandatoryFields.keys.map(_.toLowerCase)

    val vector = new java.util.Vector[InputStream]()
    for (i <- path.indices) {
      vector.add(getReader(path(i).getPath, hadoopConf))
    }

    val sequenceReader = new SequenceInputStream(vector.elements())
    val lineReader = new BufferedReader(new InputStreamReader(sequenceReader))
    var line: String = null
    var count: Int = 0

    try {
      while ( {
        line = lineReader.readLine()
        line != null && count < maxRecords
      }) {
        line = replaceLineCharsForNull(line, options)
        val records = mutable.AnyRefMap[String, Any]()
        parser.buildHashmapFromCEF(line, records)
        if (options.pivotFields) {
          val pivotFields = records.filter(r => records.contains(s"${r._1}Label"))
          val pivotFieldLabels = pivotFields.map(pf => s"${pf._1}Label").toVector
          val nonPivotFields = records.filter(r => !pivotFields.contains(r._1) && !pivotFieldLabels.contains(r._1))
          pivotFields.foreach(pf => {
            val label = records(s"${pf._1}Label").toString
            val checkedLabel = if (mandatoryFieldKeys.exists(_ == label.toLowerCase)) s"${label}_1" else label
            if (!fields.contains(checkedLabel)) {
              val dataType = getDataType(pf._1)
              fields += (checkedLabel -> dataType)
            }
          })
          updateFields(nonPivotFields, fields)
        } else {
          updateFields(records, fields)
        }
        count += 1
      }

      if (options.mode == PermissiveMode && options.corruptColumnName != null) {
        if (fields.keys.exists(_.compareToIgnoreCase(options.corruptColumnName) == 0)) {
          throw new CefRecordParserException("The specified corrupt column name conflicts with a field name in the data set.")
        }
        fields += (options.corruptColumnName -> StringType)
      }

      val fieldSchema = fields.map(item => StructField(item._1, item._2, nullable = true)).toArray

      StructType(fieldSchema)
    } finally {
      lineReader.close()
    }
  }

  private def replaceLineCharsForNull(line: String, options: CefParserOptions): String = {
    val outLine = if (line.endsWith("=")) s"$line${options.nullValue}" else line

    if (options.defensiveMode && line.contains("= ")) {
      return outLine.asInstanceOf[String].replaceAll("""([\w\d])=($|\s)""", s"$$1=${options.nullValue}$$2")
    }
    outLine.asInstanceOf[String]
  }
}