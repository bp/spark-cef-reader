package com.bp.sds

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object cef {
  implicit class CefDataFrameReader(val reader: DataFrameReader) extends AnyVal {
    def cef(path: String): DataFrame = reader.format("com.bp.sds.cef").load(path)

    def cef(paths: String*): DataFrame = reader.format("com.bp.sds.cef").load(paths:_*)
  }
}
