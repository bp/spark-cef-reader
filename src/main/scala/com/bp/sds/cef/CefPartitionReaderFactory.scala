package com.bp.sds.cef

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
    val iterator = new CefDataIterator(cefOptions)
    val reader = new PartitionReaderFromIterator[InternalRow](iterator.readFile(broadcastConf.value.value, partitionedFile, readDataSchema))

    new PartitionReaderWithPartitionValues(reader, readDataSchema, readPartitionSchema, partitionedFile.partitionValues)
  }
}
