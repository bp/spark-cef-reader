package com.bp.sds.cef

import java.io.OutputStreamWriter
import java.util.TimeZone

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types._

/**
 * Provides a writer implementation for writing data to the CEF format
 *
 * @param dataSchema schema of the data being written
 * @param writer     a writer object
 * @param cefOptions provided options to control how writing is performed
 */
private[cef] case class CefRecordWriter(dataSchema: StructType, writer: OutputStreamWriter, cefOptions: CefParserOptions) {
  private val lineSeparator = System.lineSeparator()

  private val headerFields = Vector[String](
    "cefversion",
    "devicevendor",
    "deviceproduct",
    "deviceversion",
    "signatureid",
    "name",
    "severity"
  )

  private val dateFormatIsMillis = cefOptions.isDateFormatInMillis

  private val timestampFormatter = if (dateFormatIsMillis) {
    TimestampFormatter("MMM dd yyyy HH:mm:ss.SSS zzz", TimeZone.getDefault.toZoneId, isParsing = false)
  } else {
    TimestampFormatter(cefOptions.dateFormat, TimeZone.getDefault.toZoneId, isParsing = false)
  }

  /**
   * Modifies a string value to ensure it complies with the CEF standard, this is for extension values only
   *
   * @param value the extension value to sanitize
   * @return a string formatted to meet CEF standards
   */
  private def sanitizeString(value: String): String = {
    value.replaceAll("""\\=""", "=")
      .replaceAll("""\\""", """\\\\""")
      .replaceAll("=", """\\=""")
      .replaceAll("\r\n|\r|\n", """\\n""")
  }

  /**
   * Modifies a string value to ensure it complies with the CEF standard, this is for header values only
   *
   * @param value the header value to sanitize
   * @return a string formatted to meet CEF standards
   */
  private def sanitizeHeaderValue(value: String): String = {
    value.replaceAll("""\\\|""", "|")
      .replaceAll("""\\""", """\\\\""")
      .replaceAll("""\|""", """\\|""")
      .replaceAll("\r\n|\r|\n", " ")
  }

  /**
   * Write a DataFrame row to the writer object in CEF
   *
   * @param row an [[InternalRow]] object to write
   */
  def writeRow(row: InternalRow): Unit = {
    // Validate that all of the mandatory header fields are available in the schema
    val requiredFieldCount = dataSchema.fields.count(f => headerFields.contains(f.name.toLowerCase))
    if (requiredFieldCount != headerFields.length) {
      throw new UnsupportedOperationException("Row data must contain required CEF fields")
    }

    // Split the schema fields into header and extension fields
    val headerFieldsBuffer = new Array[String](requiredFieldCount)
    val extensionFieldsBuffer = new Array[String](dataSchema.fields.length - requiredFieldCount)

    // Write each field in the row to the output writer
    var i = 0
    while (i < row.numFields) {
      val field = dataSchema.fields(i)
      val isHeader = headerFields.contains(field.name.toLowerCase)

      if (isHeader && row.isNullAt(i)) {
        // If the current field is a header field and contains a null value then the current record is invalid
        throw new UnsupportedOperationException("Mandatory fields cannot contain null values")
      } else if (isHeader) {
        // Otherwise we can write the header field to the output writer
        headerFieldsBuffer(headerFields.indexOf(field.name.toLowerCase)) = sanitizeHeaderValue(row.getString(i))
      } else if (row.isNullAt(i)) {
        // Where the current field is an extension field and has a null value then output using the user definable
        // null value
        extensionFieldsBuffer(i - requiredFieldCount) = s"${field.name}=${cefOptions.nullValue}"
      } else {
        // Otherwise, write out the extension field, converting the current value into a string representation
        //
        // Where the field is a timestamp the output using the user definable date format. If it is a string then
        // output a sanitized version of the string
        //
        // Where the data type is not supported then write a null value
        val fieldValue = field.dataType match {
          case IntegerType => row.getInt(i).toString
          case LongType => row.getLong(i).toString
          case FloatType => row.getFloat(i).toString
          case DoubleType => row.getDouble(i).toString
          case TimestampType => if (dateFormatIsMillis) {
            row.getLong(i).toString
          } else {
            timestampFormatter.format(row.getLong(i))
          }
          case StringType => sanitizeString(row.getString(i))
          case _ => cefOptions.nullValue
        }
        extensionFieldsBuffer(i - requiredFieldCount) = s"${field.name}=$fieldValue"
      }

      i += 1
    }

    // Combine the header fields and extension fields back together. This ensures that the header fields are listed
    // first and in the correct order. Also, as the headers are pipe separated, but extension fields are space separated
    // this allows the two formats to be specified.
    //
    // This leaves us with two fields, one pipe separated and the other space separated, these can then be joined using
    // a final pipe
    val combinedFields = Seq(
      headerFieldsBuffer.mkString("|"),
      extensionFieldsBuffer.mkString(" ")
    )

    writer.write(combinedFields.mkString("|"))
  }

  /**
   * Write a line ending to the output writer
   */
  def writeLineEnding(): Unit = {
    writer.write(lineSeparator)
  }
}
