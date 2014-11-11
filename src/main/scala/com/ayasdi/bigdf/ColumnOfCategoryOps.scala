/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf


class RichColumnCategory(self: Column[Short]) {
  /**
   * mark a value as NA: mutates the category to CATEGORY_NA(-1)
   */
  def markNACategory(naVal: Short): Unit = {
    resetCache
    self.rdd = self.shortRdd.map { cell => if (cell == naVal) RichColumnCategory.CATEGORY_NA else cell }
  }

  private def resetCache: Unit = {
    cachedNCats = -1
  }

  /**
   * replace NA with another category
   */
  def fillNACategory(value: Short): Unit = {
    resetCache
    self.rdd = self.shortRdd.map { cell => if (cell == RichColumnCategory.CATEGORY_NA) value else cell }
  }

  /**
   * number of categories
   */
  var cachedNCats = -1
  def nCats = {
    if(cachedNCats == -1) cachedNCats = self.shortRdd.distinct.count.toInt
    cachedNCats
  }

}

object RichColumnCategory {
  val CATEGORY_NA: Short = -1
}
