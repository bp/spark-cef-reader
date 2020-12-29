package com.bp.sds.cef

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

private[cef] class CefTable(
                             name: String,
                             sparkSession: SparkSession,
                             options: CaseInsensitiveStringMap,
                             paths: Seq[String],
                             userSpecifiedSchema: Option[StructType]
                           ) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) with Logging {
  override def formatName: String = "CEF"

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val started = LocalDateTime.now

    if (files.isEmpty) return None

    val conf = sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap)

    val schema = Some(CefRecordParser.inferSchema(files, conf, CefParserOptions.from(options)))
    val duration = ChronoUnit.SECONDS.between(started, LocalDateTime.now)
    logInfo(s"Inferring schema time taken: $duration seconds")

    schema
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    CefScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    CefOutputWriterBuilder(paths, formatName, supportsDataType, info)

  override def name(): String = name

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[CefFileSource]
}
