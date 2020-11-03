package com.bp.sds.cef

import java.io.OutputStreamWriter
import java.time.ZoneId

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types._

private[cef] case class CefRecordWriter(dataSchema: StructType, writer: OutputStreamWriter, cefOptions: CefParserOptions) {
  private val lineSeparator = "\r\n"

  private val mandatoryFields = Vector[String](
    "cefversion",
    "devicevendor",
    "deviceproduct",
    "deviceversion",
    "signatureid",
    "name",
    "severity"
  )

  private val timestampFormatter = TimestampFormatter("MMM dd yyyy HH:mm:ss.SSS zzz", ZoneId.of("UTC"), isParsing = false)
  //new DateTimeFormatterBuilder().appendPattern("MMM dd yyyy HH:mm:ss.SSS zzz").toFormatter()

  def writeRow(row: InternalRow): Unit = {
    val requiredFieldCount = dataSchema.fields.count(f => mandatoryFields.contains(f.name.toLowerCase))
    if (requiredFieldCount != mandatoryFields.length) {
      throw new UnsupportedOperationException("Row data must contain required CEF fields")
    }

    val mandatoryFieldsBuffer = new Array[String](requiredFieldCount)
    val extensionFieldsBuffer = new Array[String](dataSchema.fields.length - requiredFieldCount)

    var i = 0
    while (i < row.numFields) {
      val field = dataSchema.fields(i)
      val mandatory = mandatoryFields.contains(field.name.toLowerCase)

      if (mandatory && row.isNullAt(i)) {
        throw new UnsupportedOperationException("Mandatory fields cannot contain null values")
      } else if (mandatory) {
        mandatoryFieldsBuffer(mandatoryFields.indexOf(field.name.toLowerCase)) = row.getString(i)
      } else if (row.isNullAt(i)) {
        extensionFieldsBuffer(i - requiredFieldCount) = s"${field.name}=${cefOptions.nullValue}"
      } else {
        val fieldValue = field.dataType match {
          case IntegerType => row.getInt(i).toString
          case LongType => row.getLong(i).toString
          case FloatType => row.getFloat(i).toString
          case DoubleType => row.getDouble(i).toString
          case TimestampType => timestampFormatter.format(row.getLong(i)) // TODO: Change this to take a custom format or to use epoch seconds
          case StringType => row.getString(i).replaceAll("""\\=""", "=").replace("=", """\=""")
          case _ => cefOptions.nullValue
        }
        extensionFieldsBuffer(i - requiredFieldCount) = s"${field.name}=$fieldValue"
      }

      i += 1
    }

    val combinedFields = Seq(
      mandatoryFieldsBuffer.mkString("|"),
      extensionFieldsBuffer.mkString(" ")
    )

    writer.write(combinedFields.mkString("|"))
  }

  def writeLineEnding(): Unit = {
    writer.write(lineSeparator)
  }
}
