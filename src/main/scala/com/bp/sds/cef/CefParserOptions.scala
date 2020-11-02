package com.bp.sds.cef

import com.bp.sds.cef.CefParserOptions._
import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.spark.sql.catalyst.util.{ParseMode, PermissiveMode}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer

/**
 * Defines the options available when parsing a CEF source file
 *
 * @param maxRecords        the maximum number of records to read when inferring the schema
 * @param pivotFields       where fields have a corresponding label field, use the content of the label field as the column name
 * @param mode              parsing mode to use when reading source files
 * @param corruptColumnName the name of the column to use to store corrupt data
 * @param defensiveMode     handle corrupt lines where no value is given to the final key/value pair in a line
 * @param nullValue         defines a null value string used in the source file
 */
case class CefParserOptions(maxRecords: Int = defaultMaxRecords,
                            pivotFields: Boolean = defaultPivotFields,
                            mode: ParseMode = defaultMode,
                            corruptColumnName: String = defaultCorruptColumnName,
                            defensiveMode: Boolean = defaultDefensiveMode,
                            nullValue: String = defaultNullValue)

private[cef] object CefParserOptions {
  val defaultMaxRecords = 10000
  val defaultPivotFields = false
  val defaultMode: ParseMode = PermissiveMode
  val defaultCorruptColumnName: String = null
  val defaultDefensiveMode = false
  val defaultNullValue = "-"

  private val encoder = new DoubleMetaphone()

  private val validOptions = Map[String, String](
    encoder.encode("maxRecords") -> "maxRecords",
    encoder.encode("pivotFields") -> "pivotFields",
    encoder.encode("corruptRecordColumnName") -> "corruptRecordColumnName",
    encoder.encode("defensiveMode") -> "defensiveMode",
    encoder.encode("nullValue") -> "nullValue"
  )

  /**
   * Check user provided options for any which might be spelt incorrectly
   *
   * @param keys collection of user provided option keys
   * @return an option containing a string with an error, or [[None]] if options are valid
   */
  private def findInvalidOptions(keys: Set[String]): Option[Seq[String]] = {
    val buffer = new ArrayBuffer[String]()

    keys.foreach { key =>
      val encodedKey = encoder.encode(key)
      if (!validOptions.values.exists(_.compareToIgnoreCase(key) == 0) && validOptions.contains(encodedKey)) {
        val errorString = s"Unable to find option '$key', did you mean '${validOptions(encodedKey)}'"
        buffer.append(errorString)
      }
    }

    if (buffer.isEmpty) {
      None
    } else {
      Some(buffer)
    }
  }

  /**
   * Parse a set of parameters from a [[CaseInsensitiveStringMap]] collection
   *
   * @param options the collection of options containing possible parameter values
   * @return a [[CefParserOptions]] object
   */
  def from(options: CaseInsensitiveStringMap): CefParserOptions = {
    findInvalidOptions(options.keySet().toSet) match {
      case Some(errors) => throw new CefParserOptionsException(s"Unable to parse all options\n${errors.mkString("\n")}")
      case _ =>
    }

    val parsedMode: ParseMode = ParseMode.fromString(options.getOrDefault("mode", "permissive"))

    CefParserOptions(
      options.getInt("maxRecords", defaultMaxRecords),
      options.getBoolean("pivotFields", defaultPivotFields),
      parsedMode,
      options.getOrDefault("corruptRecordColumnName", defaultCorruptColumnName),
      options.getBoolean("defensiveMode", defaultDefensiveMode),
      options.getOrDefault("nullValue", defaultNullValue)
    )
  }

  /**
   * Parse a set of parameters from a [[Map]] collection
   *
   * @param options the collection of options containing possible parameter values
   * @return a [[CefParserOptions]] object
   */
  def from(options: Map[String, String]): CefParserOptions = {
    findInvalidOptions(options.keySet) match {
      case Some(errors) => throw new CefParserOptionsException(s"Unable to parse all options\n${errors.mkString("\n")}")
      case _ =>
    }

    val parsedMode: ParseMode = ParseMode.fromString(options.getOrElse("mode", "permissive"))

    CefParserOptions(
      options.getOrElse("maxRecords", defaultMaxRecords.toString).toInt,
      options.getOrElse("pivotFields", defaultPivotFields.toString).toBoolean,
      parsedMode,
      options.getOrElse("corruptRecordColumnName", defaultCorruptColumnName),
      options.getOrElse("defensiveMode", defaultDefensiveMode.toString).toBoolean,
      options.getOrElse("nullValue", defaultNullValue)
    )
  }
}