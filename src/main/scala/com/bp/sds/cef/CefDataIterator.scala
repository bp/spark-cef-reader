package com.bp.sds.cef

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, FailFastMode, FailureSafeParser, PermissiveMode}
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkException, TaskContext}

final class CefDataIterator(cefOptions: CefParserOptions) extends Logging {

  def readFile(conf: Configuration, file: PartitionedFile, schema: StructType): Iterator[InternalRow] = {
    val parser = new CefRecordParser(cefOptions)

    val linesReader = new HadoopFileLinesReader(file, conf)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))

    val safeParser = new FailureSafeParser[Text](
      input => Seq(parser.parseToInternalRow(input.toString, schema)),
      cefOptions.mode,
      schema,
      cefOptions.corruptColumnName
    )

    linesReader.flatMap(safeParser.parse)
  }

}