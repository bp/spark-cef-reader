package com.bp.sds.cef.utils

import java.net.URLDecoder
import scala.io.{BufferedSource, Source}

object ResourceFileUtils {
  /**
   * Gets the content of a test resource file as a string
   *
   * @param relativePath location of the test resource file
   * @return a [[String]] containing the full content of the file
   */
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

  /**
   * Gets the path of a test resource file
   *
   * @param relativePath path to the test resource file
   * @return a full path name of the requested file
   */
  def getFilePath(relativePath: String): String = {
    val path = getClass.getResource(relativePath).getPath
    URLDecoder.decode(path, "utf8")
  }
}