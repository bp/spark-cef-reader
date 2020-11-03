package com.bp.sds

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object cef {
  implicit class CefDataFrameReader(val reader: DataFrameReader) extends AnyVal {
    def cef(path: String): DataFrame = reader.format("com.bp.sds.cef").load(path)

    def cef(paths: String*): DataFrame = reader.format("com.bp.sds.cef").load(paths:_*)
  }

  implicit class CefDataFrameWriter[T](val writer: DataFrameWriter[T]) extends AnyVal {
    def cef: String => Unit = writer.format("com.bp.sds.cef").save
  }
}
