package com.bp.sds.cef

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private[cef] case class CefScanBuilder(
                                        sparkSession: SparkSession,
                                        fileIndex: PartitioningAwareFileIndex,
                                        schema: StructType,
                                        dataSchema: StructType,
                                        options: CaseInsensitiveStringMap
                                      ) extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {
  override def build(): Scan =
    CefScan(sparkSession, fileIndex, dataSchema, readDataSchema(), readPartitionSchema(), options)
}
