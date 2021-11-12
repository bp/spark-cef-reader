package com.bp.sds.cef

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.types.StructType

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
