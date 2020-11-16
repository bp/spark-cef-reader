package com.bp.sds.cef

import java.io.File
import java.sql.Timestamp
import java.util.TimeZone

import com.bp.sds.cef.utils.ResourceFileUtils
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

  behavior of "Writing a DataFrame to CEF"

  it should "write out data in the expected CEC file format" in {
    val schema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true),
      StructField("cs1Label", StringType, nullable = true),
      StructField("cs2", StringType, nullable = true),
      StructField("cs2Label", StringType, nullable = true),
      StructField("rt", TimestampType, nullable = true)
    ))

    //CEF:0|Testing|DataFrame \| Writer|1|1|Test Writing
    //cs1=This is a sample record where 0\=true cs1Label=Some label cs2=A value with | values cs2Label=Another label rt=1595194698000

    val data = 0.until(20).map { i =>
      Row(
        "CEF:0",
        "Testing",
        "DataFrame | Writer",
        "1",
        "1",
        "Test Writing",
        "3",
        s"This is a sample record where $i=true",
        "Some label",
        if (i % 3 == 0) {
          null
        } else {
          "A value with | values"
        },
        "Another label",
        Timestamp.valueOf("2020-07-19 21:38:18.000000")
      )
    }.asJava

    val outputPath = s"$outputLocation/simple-writer.log"
    val expectedContent = ResourceFileUtils.getFileContent("/cef-records/writer-expected/simple-writer.log")

    val df = spark.createDataFrame(data, schema)
    df.coalesce(1).write.mode("overwrite").option("dateFormat", "millis").cef(outputPath)

    val writtenContent = getFileContent(outputPath)

    writtenContent should be(expectedContent)
  }
}
