package com.bp.sds.cef.utils

import java.net.URI
import java.nio.file.Path

import scala.io.{BufferedSource, Source}

object ResourceFileUtils {
  def getFileContent(relativePath: String): String = {
    val scriptSourcePath = getClass.getResource(relativePath).getPath
    var scriptSource: BufferedSource = null

    try {
      scriptSource = Source.fromFile(scriptSourcePath)
      scriptSource.getLines().mkString("\n")
    } finally {
      if (scriptSource != null) scriptSource.close()
    }
  }

  def getFilePath(relativePath: String): String = {
    getClass.getResource(relativePath).getPath
  }

  def getResourceRoot: URI = {
    getClass.getResource("/").toURI
  }
}