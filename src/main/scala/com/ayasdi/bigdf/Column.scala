/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter

import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

object Preamble {
//  implicit def toColumnAny[T](col: Column[T]) = {
//    col.asInstanceOf[Column[Any]]
//  }
//
//  implicit def toRdd[T](col: Column[T]) = {
//    col.rdd
//  }
}

class Column[+T: ru.TypeTag] private(val sc: SparkContext,
                                    var rdd: RDD[Any], /* mutates due to fillNA, markNA */
                                    var index: Int) /* mutates when an orphan column is put in a DF */ {

  /**
   * count number of elements. although rdd is var not val the number of elements does not change
   */
  lazy val count = rdd.count
  val parseErrors = sc.accumulator(0L)
  /**
   * what is the column type?
   */
  val isDouble: Boolean = ru.typeOf[T] =:= ru.typeOf[Double]
  val isString: Boolean = ru.typeOf[T] =:= ru.typeOf[String]
  val getType: String = if (isDouble) "Double" else "String"

  def compareType[C: ClassTag] = {
    if (isDouble) classTag[C] == classTag[Double]
    else if (isString) classTag[C] == classTag[String]
    else false
  }

  def castDouble = {
    require(isDouble)
    this.asInstanceOf[Column[Double]]
  }

  def castString = {
    require(isString)
    this.asInstanceOf[Column[String]]
  }
  /**
   * statistical information about this column
   */
  var cachedStats: StatCounter = null

  override def toString = {
    s"rdd: ${rdd.name} index: $index type: $getType"
  }

  /**
   * print brief description of this column
   */
  def describe(): Unit = {
    val c = if (rdd != null) count else 0
    println(s"\ttype:${getType}\n\tcount:${c}\n\tparseErrors:${parseErrors}")
    if (isDouble) {
      println(s"\tmax:${stats.max}\n\tmin:${stats.min}\n\tcount:${stats.count}\n\tsum:${stats.sum}\n")
      println(s"\tmean:${stats.mean}\n\tvariance(sample):${stats.sampleVariance}\n\tstddev(sample):${stats.sampleStdev}\n")
      println(s"\tvariance:${stats.variance}\n\tstddev:${stats.stdev}")
    }
  }

  def stats = if (cachedStats != null) cachedStats
  else new DoubleRDDFunctions(rdd.asInstanceOf[RDD[Double]]).stats

  /**
   * print upto max(default 10) elements
   */
  def list(max: Int = 10): Unit = {
    println("Count: $count")
    if (isDouble) {
      if (count <= max)
        doubleRdd.collect.foreach {
          println _
        }
      else
        doubleRdd.take(max).foreach {
          println _
        }
    } else if (isString) {
      if (count <= max)
        stringRdd.collect.foreach {
          println _
        }
      else
        stringRdd.take(max).foreach {
          println _
        }
    } else {
      println("Wrong type!")
    }
  }

  /**
   * get rdd of strings to do string functions
   */
  def stringRdd = {
    if (isString) {
      rdd.asInstanceOf[RDD[String]]
    } else {
      null
    }
  }

  /**
   * distinct
   */
  def distinct = {
    rdd.distinct
  }

  /**
   * does the column have any NA
   */
  def hasNA = {
    countNA > 0
  }

  /**
   * count the number of NAs
   */
  def countNA = {
    if (isDouble) {
      rdd.asInstanceOf[RDD[Double]].filter {
        _.isNaN
      }.count
    } else {
      rdd.asInstanceOf[RDD[String]].filter {
        _.isEmpty
      }.count
    }
  }

  /**
   * mark this value as NA
   */
  def markNA(naVal: Double): Unit = {
    cachedStats = null
    if (isDouble) {
      rdd = doubleRdd.map { cell => if (cell == naVal) Double.NaN else cell}
    } else {
      println("This is not a Double column")
    }
  }

  /**
   * get rdd of doubles to use doublerddfunctions
   */
  def doubleRdd = {
    getRdd[Double]
  }

  def getRdd[R: ru.TypeTag] = {
     require(ru.typeOf[R] =:= ru.typeOf[T])
     rdd.asInstanceOf[RDD[R]]
  }

  /**
   * mark a string as NA: mutates the string to empty string
   */
  def markNA(naVal: String) {
    cachedStats = null
    if (isString) {
      rdd = stringRdd.map { cell => if (cell == naVal) "" else cell}
    } else {
      println("This is not a String column")
    }
  }

  /**
   * replace NA with another number
   */
  def fillNA(value: Double) {
    cachedStats = null
    if (isDouble) {
      rdd = doubleRdd.map { cell => if (cell.isNaN) value else cell}
    } else {
      println("This is not a Double column")
    }
  }

  /**
   * replace NA with another string
   */
  def fillNA(value: String) {
    cachedStats = null
    if (isString) {
      rdd = stringRdd.map { cell => if (cell.isEmpty) value else cell}
    } else {
      println("This is not a String column")
    }
  }

  /**
   * add two columns
   */
  def +(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.addDouble)
    else if (isString && that.isDouble)
      ColumnOfStringsOps.withColumnOfDoubles(sc, this.castString,
        that.castDouble, StringOps.addDouble)
    else null
  }

  /**
   * subtract a column from another
   */
  def -(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.subtract)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * divide a column by another
   */
  def /(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.divide)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * multiply a column with another
   */
  def *(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.multiply)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * generate a Column of boolean. true if this column is greater than another
   */
  def >>(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.gt)
    else if (isString && that.isString)
      ColumnOfStringsOps.withColumnOfString(sc, this.castString,
        that.castString, StringOps.gt)
    else null
  }

  /**
   * compare two columns
   */
  def ==(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.eqColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.eqColumn)
    else
      null
  }

  def >(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.gtColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.gtColumn)
    else
      null
  }

  def >=(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.gteColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.gteColumn)
    else
      null
  }

  def <(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.ltColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.ltColumn)
    else
      null
  }

  def <=(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.lteColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.lteColumn)
    else
      null
  }

  def !=(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.neqColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.neqColumn)
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def ==(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.eqFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def >=(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.gteFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def >(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.gtFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def <=(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.lteFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def <(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.ltFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def !=(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.neqFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def ==(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.eqFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def >=(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.gteFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def >(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.gtFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def <=(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.lteFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def <(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.ltFilter(that))
    else
      null
  }

  /**
   * compare every element in this column with a number
   */
  def !=(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.neqFilter(that))
    else
      null
  }

  /**
   * filter using custom function
   */
  def filter(f: Double => Boolean) = {
    if (isDouble)
      new DoubleColumnCondition(index, f)
    else
      null
  }

  def filter(f: String => Boolean) = {
    if (isString)
      new StringColumnCondition(index, f)
    else
      null
  }

  /**
   * apply a given function to a column to generate a new column
   * the new column does not belong to any DF automatically
   */
  def map[U: ClassTag](mapper: Any => U) = {
    val mapped = if (isDouble) {
      doubleRdd.map { row => mapper(row)}
    } else {
      stringRdd.map { row => mapper(row)}
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

  def num_map[U: ClassTag](mapper: Double => U) = {
    val mapped = if (isDouble) {
      doubleRdd.map { row => mapper(row)}
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

  def str_map[U: ClassTag](mapper: String => U) = {
    val mapped = if (isString) {
      stringRdd.map { row => mapper(row)}
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

}

object Column {
  def asDoubles(sCtx: SparkContext, stringRdd: RDD[String], index: Int, cacheLevel: StorageLevel) = {
    val col = new Column[Double](sCtx, null, index)
    val parseErrors = col.parseErrors

    val doubleRdd = stringRdd.map { x =>
      var y = Double.NaN
      try {
        y = x.toDouble
      } catch {
        case _: java.lang.NumberFormatException => parseErrors += 1
      }
      y
    }
    doubleRdd.setName(s"${stringRdd.name}.toDouble").persist(cacheLevel)
    col.rdd = doubleRdd.asInstanceOf[RDD[Any]]

    col
  }

  /**
   * create Column from existing RDD
   */
  def apply[T: ru.TypeTag](sCtx: SparkContext, rdd: RDD[T], index: Int = -1) = {
    val tpe = ru.typeOf[T]
    if (tpe =:= ru.typeOf[Double])
      newDoubleColumn(sCtx, rdd.asInstanceOf[RDD[Double]], index)
    else if (tpe =:= ru.typeOf[String])
      newStringColumn(sCtx, rdd.asInstanceOf[RDD[String]], index)
    else null
  }

  private def newDoubleColumn(sCtx: SparkContext, rdd: RDD[Double], index: Int) = {
    new Column[Double](sCtx, rdd.asInstanceOf[RDD[Any]], index)
  }

  private def newStringColumn(sCtx: SparkContext, rdd: RDD[String], index: Int) = {
    new Column[String](sCtx, rdd.asInstanceOf[RDD[Any]], index)
  }
}
