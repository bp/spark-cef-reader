package com.bp.sds.cef

import java.io.File
import java.sql.Timestamp
import java.util.TimeZone

import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

class CefDataFrameWriterTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  lazy val projectRootFile: File = new File(".").getAbsoluteFile.getParentFile
  lazy val projectRoot: String = projectRootFile.toString

  private val outputLocation = s"$projectRoot${File.separatorChar}test-output"

  def deleteRecursively(file: File): Unit = {
    if (!file.exists()) return

    file match {
      case f if f.isDirectory =>
        f.listFiles().foreach(deleteRecursively)
        f.delete()
      case _ => file.delete()
    }
  }

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("data-source-tests")
    .master("local[2]")
    .getOrCreate()

  override def beforeAll(): Unit = {
    deleteRecursively(new File(outputLocation))
  }

  override def afterAll(): Unit = {
    spark.close()

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    deleteRecursively(new File(outputLocation))
  }

  private def getNestedCauses(exception: Throwable): Seq[Throwable] = {
    if (exception.getCause != null) {
      Seq(exception) ++ getNestedCauses(exception.getCause)
    } else {
      Seq(exception)
    }
  }

  private def getFileContent(path: String, ext: String = ".log"): String = {
    val outputPathFile = new File(path)
    val partFile = outputPathFile.listFiles().filter(_.getName.endsWith(ext)).head
    val partFileSource = Source.fromFile(partFile)
    val content = partFileSource.getLines().mkString("\n")
    partFileSource.close()

    content
  }

  behavior of "Writing a DataFrame to CEF with invalid configuration"

  it should "raise an error if the mandatory fields are not provided" in {
    // Leave out CEFVersion
    val schema = StructType(Array(
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true)
    ))

    val data = Seq(
      Row("vendor", "product", "version", "sigid", "name", "sev")
    ).asJava

    val df = spark.createDataFrame(data, schema)

    val exception = the[SparkException] thrownBy df.write.mode("overwrite").cef(s"$outputLocation/missing-mandatory.log")
    val unsupportedOperationException = getNestedCauses(exception).filter(_.isInstanceOf[UnsupportedOperationException])

    unsupportedOperationException.length should be >= 1
    unsupportedOperationException.count(_.getMessage.endsWith("Row data must contain required CEF fields")) should be(1)
  }

  it should "raise an error if the mandatory fields contain null values" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", null, "name", "sev")
    ).asJava

    val df = spark.createDataFrame(data, schema)

    val exception = the[SparkException] thrownBy df.write.mode("overwrite").cef(s"$outputLocation/null-mandatory.log")
    val unsupportedOperationException = getNestedCauses(exception).filter(_.isInstanceOf[UnsupportedOperationException])

    unsupportedOperationException.length should be >= 1
    unsupportedOperationException.count(_.getMessage.endsWith("Mandatory fields cannot contain null values")) should be(1)
  }

  behavior of "Writing a DataFrame with date formats"

  it should "default to writing with year, milliseconds, and timezone" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 2020 10:17:31.964 UTC")
  }

  it should "support writing with MMM dd yyyy HH:mm:ss" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "MMM dd yyyy HH:mm:ss")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 2020 10:17:31")
  }

  it should "support writing with MMM dd yyyy HH:mm:ss.SSS" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "MMM dd yyyy HH:mm:ss.SSS")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 2020 10:17:31.964")
  }

  it should "support writing with MMM dd yyyy HH:mm:ss zzz" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "MMM dd yyyy HH:mm:ss zzz")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 2020 10:17:31 UTC")
  }

  it should "support writing with MMM dd HH:mm:ss" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "MMM dd HH:mm:ss")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 10:17:31")
  }

  it should "support writing with MMM dd HH:mm:ss.SSS zzz" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "MMM dd HH:mm:ss.SSS zzz")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 10:17:31.964 UTC")
  }

  it should "support writing with MMM dd HH:mm:ss.SSS" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "MMM dd HH:mm:ss.SSS")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 10:17:31.964")
  }

  it should "support writing with MMM dd HH:mm:ss zzz" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "MMM dd HH:mm:ss zzz")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=Oct 15 10:17:31 UTC")
  }

  it should "support writing with millis" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", TimestampType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"))
    ).asJava

    val outputPath = s"$outputLocation/timestamps-default.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("dateFormat", "millis")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=1602757051964000")
  }

  behavior of "Writing a DataFrame with valid data"

  it should "write a record out to file with mandatory fields in the correct order" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", "cs1value")
    ).asJava

    val outputPath = s"$outputLocation/expected-order.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=cs1value")
  }

  it should "write a record out to file with properly converted values" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("rt", TimestampType, nullable = true),
      StructField("cs1", IntegerType, nullable = true),
      StructField("cs2", LongType, nullable = true),
      StructField("cs3", FloatType, nullable = true),
      StructField("cs4", DoubleType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", Timestamp.valueOf("2020-10-15 10:17:31.964"), 123, 1234567890L, 1.234F, 3.1415D)
    ).asJava

    val outputPath = s"$outputLocation/value-types-check.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|rt=Oct 15 2020 10:17:31.964 UTC cs1=123 cs2=1234567890 cs3=1.234 cs4=3.1415")
  }

  it should "write a record out to file with additional equality signs escaped" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", "Issue from ip=127.0.0.01 signature=abc123==")
    ).asJava

    val outputPath = s"$outputLocation/eescaped-symbols.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("""cefversion|vendor|product|version|sigid|name|sev|cs1=Issue from ip\=127.0.0.01 signature\=abc123\=\=""")
  }

  it should "write a record out to file handling multi-line strings" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev",
        """This
          |is
          |a
          |multiline \ value
          |
          |string
          |which=bad""".stripMargin)
    ).asJava

    val outputPath = s"$outputLocation/eescaped-symbols.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("""cefversion|vendor|product|version|sigid|name|sev|cs1=This\nis\na\nmultiline \\ value\n\nstring\nwhich\=bad""")
  }

  it should "write a record out to file handling multi-line and pipe values in header strings" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion | id", """vendor \ id""", "product", "version", "sigid", "name",
        """sev
          |id
          |3""".stripMargin,
        "Value")
    ).asJava

    val outputPath = s"$outputLocation/eescaped-symbols.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("""cefversion \| id|vendor \\ id|product|version|sigid|name|sev id 3|cs1=Value""")
  }

  it should "validate mandatory fields regardless of case" in {
    val schema = StructType(Array(
      StructField("cefversion", StringType, nullable = true),
      StructField("devicevendor", StringType, nullable = true),
      StructField("deviceproduct", StringType, nullable = true),
      StructField("deviceversion", StringType, nullable = true),
      StructField("signatureid", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("severity", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", "cs1value")
    ).asJava

    val outputPath = s"$outputLocation/mandatory-case-check.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .cef(outputPath)

    succeed
  }

  it should "write out null values based on user specified value" in {
    val schema = StructType(Array(
      StructField("cefversion", StringType, nullable = true),
      StructField("devicevendor", StringType, nullable = true),
      StructField("deviceproduct", StringType, nullable = true),
      StructField("deviceversion", StringType, nullable = true),
      StructField("signatureid", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("severity", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true)
    ))

    val data = Seq(
      Row("cefversion", "vendor", "product", "version", "sigid", "name", "sev", null)
    ).asJava

    val outputPath = s"$outputLocation/null-value-check.log"

    spark.createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .option("nullValue", "NA")
      .cef(outputPath)

    val content = getFileContent(outputPath)

    content should be("cefversion|vendor|product|version|sigid|name|sev|cs1=NA")
  }
}
