package com.bp.sds.cef

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.mapAsJavaMapConverter

class CefParserOptionsTests extends AnyFlatSpec with Matchers {
  behavior of "Parsing simple map options"

  it should "return valid parser options given a valid map" in {
    val inputMap = Map[String, String](
      "maxRecords" -> "10",
      "pivotFields" -> "true",
      "corruptRecordColumnName" -> "test_col",
      "defensiveMode" -> "true",
      "nullValue" -> "NA"
    )

    val parserOptions = CefParserOptions.from(inputMap)

    parserOptions.maxRecords should be(10)
    parserOptions.pivotFields should be(true)
    parserOptions.corruptColumnName should be("test_col")
    parserOptions.defensiveMode should be(true)
    parserOptions.nullValue should be("NA")
  }

  it should "provide default options when values are not provided" in {
    val parserOptions = CefParserOptions.from(Map[String, String]())

    parserOptions.maxRecords should be(10000)
    parserOptions.pivotFields should be(false)
    parserOptions.corruptColumnName shouldBe null
    parserOptions.defensiveMode should be(false)
    parserOptions.nullValue should be("-")
  }

  it should "show a helpful error if the specified keys are not spelt correctly" in {
    val inputMap = Map[String, String](
      "macsRexord" -> "10",
      "pivotFeilds" -> "true",
      "coruptRecordColumName" -> "test_col",
      "devensiveMoad" -> "true",
      "nulVal" -> "NA"
    )

    val exception = the[CefParserOptionsException] thrownBy CefParserOptions.from(inputMap)
    exception.getMessage.contains("Unable to find option 'macsRexord', did you mean 'maxRecords'") should be(true)
    exception.getMessage.contains("Unable to find option 'pivotFeilds', did you mean 'pivotFields'") should be(true)
    exception.getMessage.contains("Unable to find option 'coruptRecordColumName', did you mean 'corruptRecordColumnName'") should be(true)
    exception.getMessage.contains("Unable to find option 'devensiveMoad', did you mean 'defensiveMode'") should be(true)
    exception.getMessage.contains("Unable to find option 'nulVal', did you mean 'nullValue'") should be(true)
  }

  behavior of "Parsing a string map"

  it should "return valid parser options given a valid map" in {
    val originalMap = Map[String, String](
      "maxRecords" -> "10",
      "pivotFields" -> "true",
      "corruptRecordColumnName" -> "test_col",
      "defensiveMode" -> "true",
      "nullValue" -> "NA"
    )

    val inputMap = new CaseInsensitiveStringMap(originalMap.asJava)

    val parserOptions = CefParserOptions.from(inputMap)

    parserOptions.maxRecords should be(10)
    parserOptions.pivotFields should be(true)
    parserOptions.corruptColumnName should be("test_col")
    parserOptions.defensiveMode should be(true)
    parserOptions.nullValue should be("NA")
  }

  it should "provide default options when values are not provided" in {
    val inputMap = new CaseInsensitiveStringMap(Map[String, String]().asJava)
    val parserOptions = CefParserOptions.from(inputMap)

    parserOptions.maxRecords should be(10000)
    parserOptions.pivotFields should be(false)
    parserOptions.corruptColumnName shouldBe null
    parserOptions.defensiveMode should be(false)
    parserOptions.nullValue should be("-")
  }

  it should "show a helpful error if the specified keys are not spelt correctly" in {
    val originalMap = Map[String, String](
      "macsRexord" -> "10",
      "pivotFeilds" -> "true",
      "coruptRecordColumName" -> "test_col",
      "devensiveMoad" -> "true",
      "nulVal" -> "NA"
    )

    val inputMap = new CaseInsensitiveStringMap(originalMap.asJava)

    val exception = the[CefParserOptionsException] thrownBy CefParserOptions.from(inputMap)
    println(exception.getMessage)
    exception.getMessage.contains("Unable to find option 'macsrexord', did you mean 'maxRecords") should be(true)
    exception.getMessage.contains("Unable to find option 'pivotfeilds', did you mean 'pivotFields") should be(true)
    exception.getMessage.contains("Unable to find option 'coruptrecordcolumname', did you mean 'corruptRecordColumnName") should be(true)
    exception.getMessage.contains("Unable to find option 'devensivemoad', did you mean 'defensiveMode'") should be(true)
    exception.getMessage.contains("Unable to find option 'nulval', did you mean 'nullValue'") should be(true)
  }
}
