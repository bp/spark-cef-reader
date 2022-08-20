package com.bp.sds.cef

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.{FileScan, TextBasedFileScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.mapAsScalaMapConverter

private[cef] case class CefScan(
                                 sparkSession: SparkSession,
                                 fileIndex: PartitioningAwareFileIndex,
                                 dataSchema: StructType,
                                 readDataSchema: StructType,
                                 readPartitionSchema: StructType,
                                 options: CaseInsensitiveStringMap,
                                 partitionFilters: Seq[Expression] = Seq.empty,
                                 dataFilters: Seq[Expression] = Seq.empty
                               ) extends TextBasedFileScan(sparkSession, options) {
  private val optionsAsScala = options.asScala.toMap
  private val cefOptions = CefParserOptions.from(options)

  override def isSplitable(path: Path): Boolean = super.isSplitable(path)

  override def createReaderFactory(): PartitionReaderFactory = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(optionsAsScala)
    val broadcastConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    CefPartitionReaderFactory(sparkSession.sessionState.conf, broadcastConf, dataSchema, readDataSchema, readPartitionSchema, cefOptions)
  }
}
