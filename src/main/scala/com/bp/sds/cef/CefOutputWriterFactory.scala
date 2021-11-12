package com.bp.sds.cef

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

private[cef] class CefOutputWriterFactory(options: Map[String, String]) extends OutputWriterFactory {
  private val cefOptions = CefParserOptions.from(options)

  override def getFileExtension(context: TaskAttemptContext): String =
    ".log" + CodecStreams.getCompressionExtension(context)

  override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter =
    new CefOutputWriter(path, cefOptions, dataSchema, context)
}
