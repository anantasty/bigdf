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
   * print upto 10 elements
   */
  def list {
    println("Count: $count")
    if (isDouble) {
      if (count <= 10)
        number.collect.foreach {
          println _
        }
      else
        number.take(10).foreach {
          println _
        }
    } else if (isString) {
      if (count <= 10)
        string.collect.foreach {
          println _
        }
      else
        string.take(10).foreach {
          println _
        }
    } else {
      println("Wrong type!")
    }
  }

  /**
   * get rdd of strings to do string functions
   */
  def string = {
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
      rdd = number.map { cell => if (cell == naVal) Double.NaN else cell}
    } else {
      println("This is not a Double column")
    }
  }

  /**
   * get rdd of doubles to use doublerddfunctions
   */
  def number = {
    if (isDouble) {
      rdd.asInstanceOf[RDD[Double]]
    } else {
      null
    }
  }

  /**
   * mark a string as NA: mutates the string to empty string
   */
  def markNA(naVal: String) {
    cachedStats = null
    if (isString) {
      rdd = string.map { cell => if (cell == naVal) "" else cell}
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
      rdd = number.map { cell => if (cell.isNaN) value else cell }
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
      rdd = string.map { cell => if (cell.isEmpty) value else cell }
    } else {
      println("This is not a String column")
    }
  }

  /**
   * add two columns
   */
  def +(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(this.asInstanceOf[Column[Double]], that.asInstanceOf[Column[Double]], DoubleOps.addDouble)
        .asInstanceOf[Column[Any]]
    else if (isString && that.isDouble)
      ColumnOfStringsOps.withColumnOfDoubles(this.asInstanceOf[Column[String]], that.asInstanceOf[Column[Double]], StringOps.addDouble)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * subtract a column from another
   */
  def -(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(this.asInstanceOf[Column[Double]], that.asInstanceOf[Column[Double]], DoubleOps.subtract)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * divide a column by another
   */
  def /(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(this.asInstanceOf[Column[Double]], that.asInstanceOf[Column[Double]], DoubleOps.divide)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * multiply a column with another
   */
  def *(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(this.asInstanceOf[Column[Double]], that.asInstanceOf[Column[Double]], DoubleOps.multiply)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * generate a Column of boolean. true if this column is greater than another
   */
  def >>(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(this.asInstanceOf[Column[Double]], that.asInstanceOf[Column[Double]], DoubleOps.gt)
    else if (isString && that.isString)
      ColumnOfStringsOps.withColumnOfString(this.asInstanceOf[Column[String]], that.asInstanceOf[Column[String]], StringOps.gt)
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
    val mapped = if(isDouble) {
       number.map { row => mapper(row) }
    } else {
       string.map { row => mapper(row) }
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

  def num_map[U: ClassTag](mapper: Double => U) = {
    val mapped = if(isDouble) {
      number.map { row => mapper(row) }
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }
  def str_map[U: ClassTag](mapper: String => U) = {
    val mapped = if(isString) {
      string.map { row => mapper(row) }
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

  private case object ColumnOfDoublesOps {
    def withColumnOfDoubles(a: Column[Double], b: Column[Double], oper: (Double, Double) => Double) = {
      val zipped = a.number.zip(b.number)
      val result = zipped.map { x => oper(x._1, x._2)}
      Column(sc, result)
    }

    def filterDouble(a: Column[Double], b: Double, oper: (Double, Double) => Boolean) = {
      val result = a.number.filter { x => oper(x, b)}
      Column(sc, result)
    }

    def withColumnOfString(a: Column[Double], b: Column[String], oper: (Double, String) => String) = {
      val zipped = a.number.zip(b.string)
      val result = zipped.map { x => oper(x._1, x._2)}
      Column(sc, result)
    }

    def withScalarDouble(a: Column[Double], b: Double, oper: (Double, Double) => Double) = {
      val result = a.number.map { x => oper(x, b)}
      Column(sc, result)
    }

    def withScalarString(a: Column[Double], b: String, oper: (Double, String) => String) = {
      val result = a.number.map { x => oper(x, b)}
      Column(sc, result)
    }
  }

  private case object ColumnOfStringsOps {
    def withColumnOfDoubles(a: Column[String], b: Column[Double], oper: (String, Double) => String) = {
      val zipped = a.string.zip(b.number)
      val result = zipped.map { x => oper(x._1, x._2)}
      Column(sc, result)
    }

    def filterDouble(a: Column[String], b: Double, oper: (String, Double) => Boolean) = {
      val result = a.string.filter { x => oper(x, b)}
      Column(sc, result)
    }

    def withColumnOfString(a: Column[String], b: Column[String], oper: (String, String) => String) = {
      val zipped = a.string.zip(b.string)
      val result = zipped.map { x => oper(x._1, x._2)}
      Column(sc, result)
    }

    def withScalarDouble(a: Column[String], b: Double, oper: (String, Double) => String) = {
      val result = a.string.map { x => oper(x, b)}
      Column(sc, result)
    }

    def withScalarString(a: Column[String], b: String, oper: (String, String) => String) = {
      val result = a.string.map { x => oper(x, b)}
      Column(sc, result)
    }
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

  private def colBool(bool: Boolean) = {
    if (bool) 1.0 else 0.0
  }

  def eq(a: Double, b: Double) = colBool(a == b)

  def neq(a: Double, b: Double) = colBool(a != b)

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

/*
 * operations with a string as first param
 */
case object StringOps {
  def addDouble(a: String, b: Double) = a + b

  def multiply(a: String, b: Double) = a * b.toInt

  def addString(a: String, b: String) = a + b

  def gt(a: String, b: String) = colBool(a > b)

  def gte(a: String, b: String) = colBool(a >= b)

  def lt(a: String, b: String) = colBool(a < b)

  private def colBool(bool: Boolean) = {
    if (bool) "t" else "f"
  }

  def lte(a: String, b: String) = colBool(a <= b)

  def eq(a: String, b: String) = colBool(a == b)

  def neq(a: String, b: String) = colBool(a != b)

  def gtFilter(b: String)(a: String) = a > b

  def gteFilter(b: String)(a: String) = a >= b

  def ltFilter(b: String)(a: String) = a < b

  def lteFilter(b: String)(a: String) = a <= b

  def eqFilter(b: String)(a: String) = a == b

  def neqFilter(b: String)(a: String) = a != b

  def gtColumn(a: String, b: String) = a > b

  def gteColumn(a: String, b: String) = a >= b

  def ltColumn(a: String, b: String) = a < b

  def lteColumn(a: String, b: String) = a <= b

  def eqColumn(a: String, b: String) = a == b

  def neqColumn(a: String, b: String) = a != b
}

object Column {
  def asDoubles(sCtx: SparkContext, stringRdd: RDD[String], index: Int, cacheLevel: StorageLevel): Column[Double] = {
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
  def apply[T: ru.TypeTag](sCtx: SparkContext, rdd: RDD[T], index: Int = -1): Column[Any] = {
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
