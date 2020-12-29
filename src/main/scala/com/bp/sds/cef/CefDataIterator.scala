package com.bp.sds.cef

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, FailFastMode, PermissiveMode}
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkException, TaskContext}

final class CefDataIterator(conf: Configuration, file: PartitionedFile, dataSchema: StructType, requiredSchema: StructType, cefOptions: CefParserOptions) extends Iterator[InternalRow] with Logging {
  private val parser = new CefRecordParser(cefOptions)

  private val bufferedReader = new HadoopFileLinesReader(file, conf)
  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => bufferedReader.close()))

  override def hasNext: Boolean = bufferedReader.hasNext

  override def next(): InternalRow = {
    try {
      parser.parseToInternalRow(bufferedReader.next().toString, requiredSchema)
    } catch {
      case e: BadRecordException =>
        logInfo(s"Invalid record found in file '${file.filePath}': ${e.getMessage}", e)
        cefOptions.mode match {
          case PermissiveMode =>
            e.partialResult() match {
              case Some(row) => row
              case None => new GenericInternalRow(requiredSchema.fields.length)
            }
          case _ =>
            logError(s"Failed to read file because of invalid record in file '${file.filePath}': ${e.getMessage}")
            throw new SparkException(
              s"""Invalid rows detected in ${file.filePath}. Parse mode ${FailFastMode.name}.
                 |To process malformed records as null try setting 'mode' to 'PERMISSIVE'.""".stripMargin, e)
        }
    }
  }
}