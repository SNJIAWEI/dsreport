package com.donson.report

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
 * Created by bigdataTeam
 */
object LoggerLevel extends Logging {

  /**
   * 设置日志级别
   * @param level
   */
  def setStreamingLogLevels(level: Level) {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(level)
    }
  }
}