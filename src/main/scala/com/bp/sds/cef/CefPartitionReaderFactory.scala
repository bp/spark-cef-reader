package com.bp.sds.cef

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderFromIterator, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

private[cef] case class CefPartitionReaderFactory(
                                                   conf: SQLConf,
                                                   broadcastConf: Broadcast[SerializableConfiguration],
                                                   dataSchema: StructType,
                                                   readDataSchema: StructType,
                                                   readPartitionSchema: StructType,
                                                   cefOptions: CefParserOptions
                                                 ) extends FilePartitionReaderFactory {
  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    val path = new Path(partitionedFile.filePath)

    val iterator = new CefDataIterator(broadcastConf.value.value, path, dataSchema, readDataSchema, cefOptions)
    val reader = new PartitionReaderFromIterator[InternalRow](iterator)

    new PartitionReaderWithPartitionValues(reader, readDataSchema, readPartitionSchema, partitionedFile.partitionValues)
  }
}
