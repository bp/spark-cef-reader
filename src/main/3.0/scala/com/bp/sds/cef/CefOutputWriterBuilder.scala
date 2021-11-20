package com.bp.sds.cef

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.v2.FileWriteBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

private[cef] case class CefOutputWriterBuilder(paths: Seq[String],
                                               formatName: String,
                                               supportsDataType: DataType => Boolean,
                                               info: LogicalWriteInfo
                                              ) extends FileWriteBuilder(paths, formatName, supportsDataType, info) {
  override def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory =
    new CefOutputWriterFactory(options)
}
