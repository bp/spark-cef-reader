package com.bp.sds.cef

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile, TextBasedFileFormat}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

private[cef] class CefFileSource extends TextBasedFileFormat with DataSourceRegister {
  override def shortName(): String = "cef"

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean =
    super.isSplitable(sparkSession, options, path)

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) return None

    val conf = sparkSession.sessionState.newHadoopConfWithOptions(options)

    Some(CefRecordParser.inferSchema(files, conf, CefParserOptions.from(options)))
  }

  override protected def buildReader(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val broadcastHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      new CefDataIterator(broadcastHadoopConf.value.value, file, dataSchema, requiredSchema, CefParserOptions.from(options))
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory =
    new CefOutputWriterFactory(options)
}


