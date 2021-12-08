package com.bp.sds.cef

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

private[cef] case class CefOutputWriterBuilder(paths: Seq[String],
                                               formatName: String,
                                               supportsDataType: DataType => Boolean,
                                               info: LogicalWriteInfo
                                              ) extends FileWrite with WriteBuilder {
  override def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory =
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String =
        ".log" + CodecStreams.getCompressionExtension(context)

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter =
        new CefOutputWriter(path, CefParserOptions.from(options), dataSchema, context)
    }
}
