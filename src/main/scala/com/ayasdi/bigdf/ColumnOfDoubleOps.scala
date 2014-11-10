/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.SparkContext

private[bigdf] case object ColumnOfDoublesOps {
  def withColumnOfDoubles(sc: SparkContext, a: Column[Double], b: Column[Double], oper: (Double, Double) => Double) = {
    val zipped = a.doubleRdd.zip(b.doubleRdd)
    val result = zipped.map { x => oper(x._1, x._2)}
    Column(sc, result)
  }

  def filterDouble(sc: SparkContext, a: Column[Double], b: Double, oper: (Double, Double) => Boolean) = {
    val result = a.doubleRdd.filter { x => oper(x, b)}
    Column(sc, result)
  }

  def withColumnOfString(sc: SparkContext, a: Column[Double], b: Column[String], oper: (Double, String) => String) = {
    val zipped = a.doubleRdd.zip(b.stringRdd)
    val result = zipped.map { x => oper(x._1, x._2)}
    Column(sc, result)
  }

  def withScalarDouble(sc: SparkContext, a: Column[Double], b: Double, oper: (Double, Double) => Double) = {
    val result = a.doubleRdd.map { x => oper(x, b)}
    Column(sc, result)
  }

  def withScalarString(sc: SparkContext, a: Column[Double], b: String, oper: (Double, String) => String) = {
    val result = a.doubleRdd.map { x => oper(x, b)}
    Column(sc, result)
  }
}


/*
 * operations with a double as first param
 */
case object DoubleOps {
  def addDouble(a: Double, b: Double) = a + b

  def subtract(a: Double, b: Double) = a - b

  def divide(a: Double, b: Double) = a / b

  def multiply(a: Double, b: Double) = a * b

  def addString(a: Double, b: String) = a + b

  def gt(a: Double, b: Double) = colBool(a > b)

  def gte(a: Double, b: Double) = colBool(a >= b)

  def lt(a: Double, b: Double) = colBool(a < b)

  def lte(a: Double, b: Double) = colBool(a <= b)

  def eq(a: Double, b: Double) = colBool(a == b)

  def neq(a: Double, b: Double) = colBool(a != b)

  private def colBool(bool: Boolean) = {
    if (bool) 1.0 else 0.0
  }

  def gtFilter(b: Double)(a: Double) = a > b

  def gteFilter(b: Double)(a: Double) = a >= b

  def ltFilter(b: Double)(a: Double) = a < b

  def lteFilter(b: Double)(a: Double) = a <= b

  def eqFilter(b: Double)(a: Double) = a == b

  def neqFilter(b: Double)(a: Double) = a != b

  def gtColumn(a: Double, b: Double) = a > b

  def gteColumn(a: Double, b: Double) = a >= b

  def ltColumn(a: Double, b: Double) = a < b

  def lteColumn(a: Double, b: Double) = a <= b

  def eqColumn(a: Double, b: Double) = a == b

  def neqColumn(a: Double, b: Double) = a != b
}
