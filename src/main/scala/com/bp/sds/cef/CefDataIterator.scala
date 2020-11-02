package com.bp.sds.cef

import java.io.{BufferedReader, InputStreamReader}
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, FailFastMode, PermissiveMode}
import org.apache.spark.sql.types.StructType

final class CefDataIterator(conf: Configuration, path: Path, dataSchema: StructType, requiredSchema: StructType, cefOptions: CefParserOptions) extends Iterator[InternalRow] with Logging {
  private val parser = new CefRecordParser(cefOptions)
  private var started: LocalDateTime = _

  private val bufferedReader = new BufferedReader(new InputStreamReader(CefRecordParser.getReader(path, conf)))

  private var line: String = _

  override def hasNext: Boolean = {
    if (started == null) {
      started = LocalDateTime.now()
    }
    line = bufferedReader.readLine()

    if (line == null) {
      if (started != null) {
        val timeTaken = ChronoUnit.SECONDS.between(started, LocalDateTime.now)
        logInfo(s"Processing file '${path.toUri}' took $timeTaken seconds")
      }
      logInfo(s"Completed reading file '${path.toUri}'")
      bufferedReader.close()
      false
    } else {
      true
    }
  }

  override def next(): InternalRow = {
    try {
      parser.parseToInternalRow(line, requiredSchema)
    } catch {
      case e: BadRecordException => cefOptions.mode match {
        case PermissiveMode =>
          e.partialResult() match {
            case Some(row) => row
            case None => new GenericInternalRow(requiredSchema.fields.length)
          }
        case FailFastMode =>
          throw new SparkException(
            s"""Invalid rows detected in ${path.toUri}. Parse mode ${FailFastMode.name}.
               |To process malformed records as null try setting 'mode' to 'PERMISSIVE'.""".stripMargin)
      }
    }
  }
}
