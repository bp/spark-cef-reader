package com.bp.sds.cef

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private[cef] class DefaultSource extends FileDataSourceV2 with DataSourceRegister {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[CefFileSource]

  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)

    new CefTable(tableName, sparkSession, options, paths, None)
  }

  override def shortName(): String = "cef"
}
