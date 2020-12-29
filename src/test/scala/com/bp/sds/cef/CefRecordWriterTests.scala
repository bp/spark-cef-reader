package com.bp.sds.cef

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.sql.Timestamp
import java.util.TimeZone

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CefRecordWriterTests extends AnyFlatSpec with Matchers {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private val headerFields = Array(
    StructField("CEFVersion", StringType, nullable = true),
    StructField("DeviceVendor", StringType, nullable = true),
    StructField("DeviceProduct", StringType, nullable = true),
    StructField("DeviceVersion", StringType, nullable = true),
    StructField("SignatureID", StringType, nullable = true),
    StructField("Name", StringType, nullable = true),
    StructField("Severity", StringType, nullable = true)
  )

  private def withOutputStream()(f: (ByteArrayOutputStream, OutputStreamWriter) => Unit): Unit = {
    val out = new ByteArrayOutputStream()
    val stream = new OutputStreamWriter(out, "UTF-8")

    try {
      f(out, stream)
    } finally {
      stream.close()
      out.close()
    }
  }

  behavior of "Writing records to CEF with invalid data"

  it should "raise an error if the header fields contain null values" in {
    val schema = StructType(headerFields)

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      null,
      UTF8String.fromString("name"),
      UTF8String.fromString("sev")
    ))

    withOutputStream() { (_, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))

      val exception = the[UnsupportedOperationException] thrownBy recordWriter.writeRow(data)
      exception.getMessage should be("Mandatory fields cannot contain null values")
    }
  }

  it should "raise an error if the cef version header does not contain the CEF version data" in {
    val schema = StructType(headerFields)

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("cef version 0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev")
    ))

    withOutputStream() { (_, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))

      val exception = the[UnsupportedOperationException] thrownBy recordWriter.writeRow(data)
      exception.getMessage should be("CEFVersion field must end with the CEF version information, e.g. 'CEF:0'")
    }
  }

  behavior of "Writing a record with date formats"

  private def testDateFormatting(format: Option[String], expectedFormattedDate: String): Unit = {
    val schema = StructType(headerFields ++ Seq(StructField("cs1", TimestampType, nullable = true)))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ))

    withOutputStream() { (out, stream) =>
      val options = format match {
        case None => CefParserOptions.from(Map[String, String]())
        case Some(fmt) => CefParserOptions.from(Map[String, String]("dateFormat" -> fmt))
      }
      val recordWriter = CefRecordWriter(schema, stream, options)
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be(s"CEF:0|vendor|product|version|sigid|name|sev|cs1=$expectedFormattedDate")
    }
  }

  it should "default to writing with year, milliseconds, and timezone" in {
    testDateFormatting(None, "Oct 15 2020 10:17:31.964 UTC")
  }

  it should "support writing with supported formats" in {
    testDateFormatting(Some("MMM dd yyyy HH:mm:ss"), "Oct 15 2020 10:17:31")
    testDateFormatting(Some("MMM dd yyyy HH:mm:ss.SSS"), "Oct 15 2020 10:17:31.964")
    testDateFormatting(Some("MMM dd yyyy HH:mm:ss zzz"), "Oct 15 2020 10:17:31 UTC")
    testDateFormatting(Some("MMM dd yyyy HH:mm:ss.SSS zzz"), "Oct 15 2020 10:17:31.964 UTC")
    testDateFormatting(Some("MMM dd HH:mm:ss"), "Oct 15 10:17:31")
    testDateFormatting(Some("MMM dd HH:mm:ss.SSS"), "Oct 15 10:17:31.964")
    testDateFormatting(Some("MMM dd HH:mm:ss zzz"), "Oct 15 10:17:31 UTC")
    testDateFormatting(Some("MMM dd HH:mm:ss.SSS zzz"), "Oct 15 10:17:31.964 UTC")
    testDateFormatting(Some("millis"), "1602757051964000")
  }

  behavior of "Writing a DataFrame with valid data"

  it should "write a record out to file with mandatory fields in the correct order" in {
    val schema = StructType(headerFields ++ Seq(StructField("cs1", StringType, nullable = true)))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      UTF8String.fromString("cs1value")
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("CEF:0|vendor|product|version|sigid|name|sev|cs1=cs1value")
    }
  }

  it should "write a record out to file with properly converted values" in {
    val schema = StructType(headerFields ++ Seq(
      StructField("rt", TimestampType, nullable = true),
      StructField("cs1", IntegerType, nullable = true),
      StructField("cs2", LongType, nullable = true),
      StructField("cs3", FloatType, nullable = true),
      StructField("cs4", DoubleType, nullable = true)
    ))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2020-10-15 10:17:31.964")),
      123,
      1234567890L,
      1.234F,
      3.1415D
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("CEF:0|vendor|product|version|sigid|name|sev|rt=Oct 15 2020 10:17:31.964 UTC cs1=123 cs2=1234567890 cs3=1.234 cs4=3.1415")
    }
  }

  it should "write a record out to file with additional equality signs escaped" in {
    val schema = StructType(headerFields ++ Seq(StructField("cs1", StringType, nullable = true)))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      UTF8String.fromString("Issue from ip=127.0.0.01 signature=abc123==")
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("""CEF:0|vendor|product|version|sigid|name|sev|cs1=Issue from ip\=127.0.0.01 signature\=abc123\=\=""")
    }
  }

  it should "write a record out to file handling multi-line strings" in {
    val schema = StructType(headerFields ++ Seq(StructField("cs1", StringType, nullable = true)))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      UTF8String.fromString("""This
                              |is
                              |a
                              |multiline \ value
                              |
                              |string
                              |which=bad""".stripMargin)
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("""CEF:0|vendor|product|version|sigid|name|sev|cs1=This\nis\na\nmultiline \\ value\n\nstring\nwhich\=bad""")
    }
  }

  it should "write a record out to file handling multi-line and pipe values in header strings" in {
    val schema = StructType(headerFields ++ Seq(StructField("cs1", StringType, nullable = true)))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("""vendor \ id"""),
      UTF8String.fromString("product | id"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("""sev
                              |id
                              |3""".stripMargin),
      UTF8String.fromString("Value")
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("""CEF:0|vendor \\ id|product \| id|version|sigid|name|sev id 3|cs1=Value""")
    }
  }

  it should "validate mandatory fields regardless of case" in {
    val schema = StructType(headerFields.map(f => StructField(f.name.toLowerCase, f.dataType, f.nullable)))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev")
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()
    }

    succeed
  }

  it should "write out null values based on user specified value" in {
    val schema = StructType(headerFields ++ Seq(StructField("cs1", StringType, nullable = true)))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      null
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]("nullValue" -> "NA")))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("CEF:0|vendor|product|version|sigid|name|sev|cs1=NA")
    }
  }

  it should "write out a record in the correct header order" in {
    val schema = StructType((headerFields ++ Seq(StructField("cs1", StringType, nullable = true))).reverse)

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      UTF8String.fromString("cs1Value")
    ).reverse)

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("CEF:0|vendor|product|version|sigid|name|sev|cs1=cs1Value")
    }
  }

  it should "correctly escape values in the header" in {
    val schema = StructType(headerFields)

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("spaces should not be escaped"),
      UTF8String.fromString("Backslashes \\ should be escaped"),
      UTF8String.fromString("Pipes | should be escaped"),
      UTF8String.fromString("Equal signs = should not be escaped"),
      UTF8String.fromString("name"),
      UTF8String.fromString(
        """multiline
          |strings
          |should
          |be
          |collapsed""".stripMargin)
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("""CEF:0|spaces should not be escaped|Backslashes \\ should be escaped|Pipes \| should be escaped|Equal signs = should not be escaped|name|multiline strings should be collapsed|""")
    }
  }

  it should "correctly escape values in the extensions" in {
    val schema = StructType(headerFields ++ Seq(
      StructField("cs1", StringType, nullable = true),
      StructField("cs2", StringType, nullable = true),
      StructField("cs3", StringType, nullable = true),
      StructField("cs4", StringType, nullable = true)
    ))

    val data = InternalRow.fromSeq(Seq(
      UTF8String.fromString("CEF:0"),
      UTF8String.fromString("vendor"),
      UTF8String.fromString("product"),
      UTF8String.fromString("version"),
      UTF8String.fromString("sigid"),
      UTF8String.fromString("name"),
      UTF8String.fromString("sev"),
      UTF8String.fromString("Pipes | should not be escaped"),
      UTF8String.fromString("Backslashes \\ should be escaped"),
      UTF8String.fromString("Equal signs = should be escaped"),
      UTF8String.fromString(
        """multiline
          |strings
          |should be
          |escaped""".stripMargin)
    ))

    withOutputStream() { (out, stream) =>
      val recordWriter = CefRecordWriter(schema, stream, CefParserOptions.from(Map[String, String]()))
      recordWriter.writeRow(data)
      stream.flush()
      out.flush()

      out.toString should be("""CEF:0|vendor|product|version|sigid|name|sev|cs1=Pipes | should not be escaped cs2=Backslashes \\ should be escaped cs3=Equal signs \= should be escaped cs4=multiline\nstrings\nshould be\nescaped""")
    }
  }
}
