/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.SparkContext

object PythonDF {
  def buildDF(sc: SparkContext, name: String, separator: String, fasterGuess: Boolean): DF = {
    val sep : Char = separator.charAt(0)
    DF(sc, name, sep, fasterGuess)
  }
}
