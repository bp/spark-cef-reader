package com.bp.sds.cef

import java.io.File
import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.TimeZone

import com.bp.sds.cef.utils.ResourceFileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CefDataFrameReaderTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("data-source-tests")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  behavior of "Reading a CEF source file"

  it should "should correctly handle information from Citrix systems" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/citrix-events.cef")

    import spark.implicits._

    val df = spark.read
      .cef(sourceFile)
      .cache()

    try {
      df.count() should be(4)
      df.filter($"CEFVersion".isNull).count() should be(0)
      df.filter($"act" === "not blocked").count() should be(2)
      df.filter($"act" === "transformed").count() should be(2)
      df.filter(substring($"msg", 0, 1) === " ").count() should be(0)
    } finally {
      df.unpersist()
    }
  }

  it should "should correctly handle information from FireEye-Splunk systems" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/fireeye-splunk-events.cef")

    import spark.implicits._

    val df = spark.read
      .cef(sourceFile)
      .cache()

    val expectedSchema = StructType(Array(
      StructField("CEFVersion", StringType, nullable = true),
      StructField("DeviceVendor", StringType, nullable = true),
      StructField("DeviceProduct", StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("SignatureID", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Severity", StringType, nullable = true),
      StructField("dvchost", StringType, nullable = true),
      StructField("src", StringType, nullable = true),
      StructField("dmac", StringType, nullable = true),
      StructField("proto", StringType, nullable = true),
      StructField("dproc", StringType, nullable = true),
      StructField("smac", StringType, nullable = true),
      StructField("shost", StringType, nullable = true),
      StructField("externalId", StringType, nullable = true),
      StructField("dvc", StringType, nullable = true),
      StructField("dst", StringType, nullable = true),
      StructField("cn1", LongType, nullable = true),
      StructField("cn1Label", StringType, nullable = true),
      StructField("cs1", StringType, nullable = true),
      StructField("cs1Label", StringType, nullable = true),
      StructField("cs2", StringType, nullable = true),
      StructField("cs2Label", StringType, nullable = true),
      StructField("cs3", StringType, nullable = true),
      StructField("cs3Label", StringType, nullable = true),
      StructField("cs4", StringType, nullable = true),
      StructField("cs4Label", StringType, nullable = true),
      StructField("filePath", StringType, nullable = true),
      StructField("rt", TimestampType, nullable = true),
      StructField("dpt", IntegerType, nullable = true),
      StructField("spt", IntegerType, nullable = true)
    ))

    try {
      val expectedDate = Timestamp.from(ZonedDateTime.of(2014, 2, 2, 16, 57, 47, 0, ZoneId.of("UTC")).toInstant)

      df.count() should be(10)
      df.schema.fields should contain allElementsOf expectedSchema.fields
      df.filter($"rt" === expectedDate).count() should be(10)
      df.filter($"smac" === "XX:XX:XX:XX:XX:XX").count() should be(10)
    } finally {
      df.unpersist()
    }
  }

  it should "handle files with varying schemas per row" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef")

    import spark.implicits._

    val df = spark.read
      .cef(sourceFile)
      .cache()

    try {
      df.schema.fields.count(f => f.name == "randomValue") should be(1)
      df.schema.fields.count(f => f.name == "cs11")

      df.filter($"randomValue".isNotNull).count() should be(1)
      df.filter($"cs11".isNotNull).count() should be(1)
    } finally {
      df.unpersist()
    }
  }

  it should "correctly assign data types to records with known extensions" in {
    val recordSource = ResourceFileUtils.getFilePath("/cef-records/type-tests.cef")

    val df = spark.read
      .cef(recordSource)

    df.schema.fields.filter(f => f.name == "eventId").head.dataType shouldBe a[LongType]
    df.schema.fields.filter(f => f.name == "cn1").head.dataType shouldBe a[LongType]
    df.schema.fields.filter(f => f.name == "cfp1").head.dataType shouldBe a[FloatType]
    df.schema.fields.filter(f => f.name == "slong").head.dataType shouldBe a[DoubleType]
    df.schema.fields.filter(f => f.name == "slat").head.dataType shouldBe a[DoubleType]
    df.schema.fields.filter(f => f.name == "rt").head.dataType shouldBe a[TimestampType]
  }

  it should "read files from a gzip source" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef.gz")

    val df = spark.read
      .cef(sourceFile)

    df.count() should be(3)
  }

  it should "read files from a bzip2 source" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef.bz2")

    val df = spark.read
      .cef(sourceFile)

    df.count() should be(3)
  }

  it should "read files from multiple files" in {
    val sourceFile1 = ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef.gz")
    val sourceFile2 = ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef")

    val df = spark.read
      .cef(sourceFile1, sourceFile2)

    df.count() should be(6)
  }

  it should "read files using a glob pattern" in {
    val inputFile = new File(ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef.gz")).getParent
    val globPath = s"$inputFile${File.separatorChar}imperva*"

    val df = spark.read
      .cef(globPath)

    df.count() should be(9)
  }

  it should "be registered as a SQL data provider" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef.gz")

    val df = spark.sql(s"SELECT * FROM cef.`$sourceFile`")

    df.count() should be(3)
  }

  it should "correctly handle the max records for inferring a schema when provided" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/imperva-access-event.cef")

    val df = spark.read
      .option("maxRecords", 1)
      .cef(sourceFile)

    df.schema.fields.count(f => f.name == "randomValue") should be(0)
    df.schema.fields.count(f => f.name == "cs11") should be(1)
  }

  it should "handle instances where the extensions contain keys which conflict with the required fields" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/conflict-name-test.cef")

    val df = spark.read
      .cef(sourceFile)

    df.schema.fields.exists(_.name == "Severity") should be(true)
    df.schema.fields.exists(_.name == "severity_1") should be(true)

    df.select("severity_1").head.getString(0) should be("1")
  }

  it should "handle paths with spaces in the name" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/citrix events spaces in name.cef")

    val df = spark.read.cef(sourceFile)

    df.count() should be(4)
  }

  behavior of "Reading files with value label pairs"

  it should "read the pairs as separate columns by default" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/pivot-test-events.cef")

    val df = spark.read
      .cef(sourceFile)

    df.schema.fields.count(f => f.name == "field1") should be(1)
    df.schema.fields.count(f => f.name == "field1Label") should be(1)

    df.count() should be(10)
  }

  it should "pivot the pairs into a single column" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/pivot-test-events.cef")

    import spark.implicits._

    val df = spark.read
      .option("pivotFields", value = true)
      .cef(sourceFile)
      .cache()

    try {
      df.schema.fields.count(f => f.name == "field1") should be(0)
      df.schema.fields.count(f => f.name == "field1Label") should be(0)
      df.schema.fields.count(f => f.name == "Pivot Header") should be(1)

      df.count() should be(10)
      df.filter($"Pivot Header" === "901").count() should be(1)
    } finally {
      df.unpersist()
    }
  }

  it should "pivot pairs into column, but keep originals if they are not paired" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/pivot-test-events-partial.cef")

    val df = spark.read
      .option("pivotFields", value = true)
      .cef(sourceFile)

    df.schema.fields.count(f => f.name == "field1") should be(1)
    df.schema.fields.count(f => f.name == "field1Label") should be(1)
    df.schema.fields.count(f => f.name == "Pivot Header") should be(1)
  }

  it should "pivot pairs into column and maintain data types" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/pivot-test-events-types.cef")

    import org.apache.spark.sql.functions.year
    import spark.implicits._

    val df = spark.read
      .option("pivotFields", value = true)
      .cef(sourceFile)
      .cache()

    try {
      df.schema.fields.count(f => f.name == "Start Date") should be(1)
      df.schema.fields.filter(f => f.name == "Start Date").head.dataType shouldBe a[TimestampType]
      df.schema.fields.count(f => f.name == "deviceValue") should be(1)
      df.schema.fields.filter(f => f.name == "deviceValue").head.dataType shouldBe a[FloatType]

      df.schema.fields.count(f => f.name == "startLabel") should be(0)
      df.schema.fields.count(f => f.name == "start") should be(0)

      df.filter($"Start Date".isNull).count() should be(1)
      df.filter($"deviceValue".isNull).count() should be(0)

      df.filter($"Start Date" === "2020-08-13 10:01:46").count() should be(5)
      df.filter(year($"Start Date") === 1970).count() should be(4)
    } finally {
      df.unpersist()
    }
  }

  it should "handle conflicting field names when pivoting fields" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/conflict-pivot-name-test.cef")

    import spark.implicits._

    val df = spark.read
      .option("pivotFields", value = true)
      .cef(sourceFile)
      .cache()

    try {
      df.schema.fields.count(f => f.name == "Severity") should be(1)
      df.schema.fields.count(f => f.name == "severity_1") should be(1)

      df.select($"severity_1").head.getString(0) should be("1.234")
    } finally {
      df.unpersist()
    }
  }

  it should "correctly handle fields when null values are empty strings in defensive mode" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/invalid-cef-defensive.cef")

    import spark.implicits._

    val df = spark.read
      .option("defensiveMode", value = true)
      .cef(sourceFile)

    df.printSchema()
    df.filter($"field1Label".isNull).count() should be(1)
    df.filter($"nullEntry".isNull).count() should be(0)
  }

  it should "correctly handle fields when null values are empty strings and specified null value in defensive mode" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/invalid-cef-defensive.cef")

    import spark.implicits._

    val df = spark.read
      .option("defensiveMode", value = true)
      .option("nullValue", "NA")
      .cef(sourceFile)

    df.printSchema()
    df.filter($"field1Label".isNull).count() should be(1)
    df.filter($"nullEntry".isNull).count() should be(1)
    df.filter($"filehash" === "aGFzaA==").count() should be(1)
  }

  it should "correctly handle the last field when null values contain a string terminator in defensive mode" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/invalid-cef-defensive.cef")

    import spark.implicits._

    val df = spark.read
      .option("defensiveMode", value = true)
      .cef(sourceFile)

    df.filter($"test".isNull).count() should be(1)
  }

  it should "correctly handle multiple pipes in the record, and escaped pipes in the header" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/header-pipes-tests.cef")

    import spark.implicits._

    val df = spark.read
      .cef(sourceFile)

    df.filter($"Name" === """Invalid \| Data""").count() should be(1)
    df.filter($"cs1" === "This|is some|test|data").count() should be(3)
  }

  behavior of "Processing files with corrupt records"

  it should "capture the corrupt record in passive mode" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/corrupt-required-fields.cef")

    import spark.implicits._

    val df = spark.read
      .option("maxRecords", 1) // Only read the first record otherwise this will fail during schema inference
      .option("mode", "permissive")
      .option("corruptRecordColumnName", "_corrupt_record")
      .cef(sourceFile)

    df.filter($"_corrupt_record".isNotNull).count() should be(1)
  }

  it should "display a partial result in passive mode if the corrupt column name is not specified" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/corrupt-required-fields.cef")

    import spark.implicits._

    val df = spark.read
      .option("maxRecords", 1) // Only read the first record otherwise this will fail during schema inference
      .option("mode", "permissive")
      .cef(sourceFile)

    df.filter($"Severity".isNull).count() should be(1)
  }

  it should "Fail quickly in fail-fast mode" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/corrupt-required-fields.cef")

    val df = spark.read
      .option("maxRecords", 1) // Only read the first record otherwise this will fail during schema inference
      .option("mode", "failfast")
      .cef(sourceFile)

    val error = the[SparkException] thrownBy df.show()
    error.getMessage.contains("com.bp.sds.cef.CefRecordParserException: Missing") should be(true)
  }

  it should "Drop malformed records in drop-malformed mode" in {
    val sourceFile = ResourceFileUtils.getFilePath("/cef-records/corrupt-required-fields.cef")

    val df = spark.read
      .option("maxRecords", 1) // Only read the first record otherwise this will fail during schema inference
      .option("mode", "dropmalformed")
      .cef(sourceFile)

    df.count() should be(2)
  }

}