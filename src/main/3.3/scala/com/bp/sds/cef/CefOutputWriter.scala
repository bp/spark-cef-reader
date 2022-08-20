package com.bp.sds.cef

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

import java.nio.charset.StandardCharsets

private[cef] class CefOutputWriter(val path: String, cefOptions: CefParserOptions, dataSchema: StructType, context: TaskAttemptContext) extends OutputWriter with Logging {
  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), StandardCharsets.UTF_8)
  private val gen = CefRecordWriter(dataSchema, writer, cefOptions)

  override def write(row: InternalRow): Unit = {
    gen.writeRow(row)
    gen.writeLineEnding()
  }

  override def close(): Unit = {
    writer.close()
  }
}
