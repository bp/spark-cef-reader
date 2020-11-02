package com.bp.sds.cef

import scala.collection.mutable

case class CefRecordParserException(message: String, partialData: Option[mutable.AnyRefMap[String, Any]], cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, None, null)
  }

  def this(message: String, partialData: mutable.AnyRefMap[String, Any]) {
    this(message, Some(partialData), null)
  }
}
