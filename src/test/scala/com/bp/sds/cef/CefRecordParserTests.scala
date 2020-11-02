package com.bp.sds.cef

import com.bp.sds.cef.utils.ResourceFileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructField}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CefRecordParserTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("data-source-tests")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  behavior of "Parsing a single record"

  it should "correctly extract data from an imperva access event" in {
    val recordSource = ResourceFileUtils.getFileContent("/cef-records/type-tests.cef").split("\n")

    val fields = Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("eventId", LongType, nullable = true),
      StructField("cn1", LongType, nullable = true),
      StructField("cfp1", FloatType, nullable = true)
    )

    val recordParser = new CefRecordParser(CefParserOptions(maxRecords = 10))
    val result = recordSource.map(s => recordParser.parse(s, fields))

    result(0).exists(_._1.compareToIgnoreCase("cfp1") == 0) should be(true)
    result(0).filter(_._1.compareToIgnoreCase("cfp1") == 0).head._2 should be(1.0)
  }

  behavior of "Inferring a schema"

  it should "pivot fields where a Label field exists when requested" in {
    val recordSource = new Path(ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef"))

    val conf = spark.sessionState.newHadoopConf()
    val fs = recordSource.getFileSystem(conf)
    val options = new CefParserOptions(maxRecords = 10, pivotFields = true)

    val files = fs.globStatus(recordSource)

    val schema = CefRecordParser.inferSchema(files, conf, options)

    schema.fields.count(f => f.name == "cs3") should be(0)
    schema.fields.count(f => f.name == "cs3Label") should be(0)
    schema.fields.count(f => f.name == "CO Support") should be(1)
  }

}
