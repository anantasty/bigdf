/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
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
    implicit def toColumnAny[T](col: Column[T]) = { col.asInstanceOf[Column[Any]] }
    implicit def toRdd[T](col: Column[T]) = { col.rdd }
}

case class Column[T: ru.TypeTag] private (var rdd: RDD[T], /* mutates due to fillNA, markNA */
                                          var index: Int, /* mutates when an orphan column is put in a DF */
                                          parseErrors: Long) {

    override def toString() = {
        val c = if (rdd != null) count else 0
      s"\ttype:${getType}\n\tcount:${c}\n\tparseErrors:${parseErrors}"
    }

    /**
     *  statistical information about this column
     */
    var cachedStats: StatCounter = null
    def stats = if (cachedStats != null) cachedStats
    else new DoubleRDDFunctions(rdd.asInstanceOf[RDD[Double]]).stats


  /**
   * what is the column type?
   */
  def isDouble: Boolean = ru.typeOf[T] =:= ru.typeOf[Double]

  def isString: Boolean = ru.typeOf[T] =:= ru.typeOf[String]

  def getType: String = if (isDouble) "Double" else "String"

  /**
   * print brief description of this column
     */
    def describe() {
        println(toString)
    if (isDouble) {
            println(s"\tmax:${stats.max}\n\tmin:${stats.min}\n\tcount:${stats.count}\n\tsum:${stats.sum}\n")
            println(s"\tmean:${stats.mean}\n\tvariance(sample):${stats.sampleVariance}\n\tstddev(sample):${stats.sampleStdev}\n")
            println(s"\tvariance:${stats.variance}\n\tstddev:${stats.stdev}")
        }
    }

    /**
     * print upto 10 elements
     */
    def list {
        println("Count: $count")
      if (isDouble) {
            if (count <= 10)
                number.collect.foreach { println _ }
            else
                number.take(10).foreach { println _ }
      } else if (isString) {
            if (count <= 10)
                string.collect.foreach { println _ }
            else
                string.take(10).foreach { println _ }
        } else {
            println("Wrong type!")
        }
    }

    /**
     * count number of elements. although rdd is var not val the number of elements does not change
     */
    lazy val count = rdd.count

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
     * mark this value as NA
     */
    def markNA(naVal: Double) {
        cachedStats = null
      if (isDouble) {
            val col = this.asInstanceOf[Column[Double]]
            rdd = col.rdd.map { cell => if (cell == naVal) Double.NaN else cell }.asInstanceOf[RDD[T]]
        } else {
            println("This is not a Double column")
        }
    }

    /**
     * mark a string as NA: mutates the string to empty string
     */
    def markNA(naVal: String) {
        cachedStats = null
      if (isString) {
            val col = this.asInstanceOf[Column[String]]
            rdd = col.rdd.map { cell => if (cell == naVal) "" else cell }.asInstanceOf[RDD[T]]
        } else {
            println("This is not a String column")
        }
    }

    /**
     * count the number of NAs
     */
    def countNA = {
      if (isDouble) {
            rdd.asInstanceOf[RDD[Double]].filter { _.isNaN }.count
        } else {
            rdd.asInstanceOf[RDD[String]].filter { _.isEmpty }.count
        }
    }

    /**
     * replace NA with another number
     */
    def fillNA(value: Double) {
        cachedStats = null
      if (isDouble) {
            val col = this.asInstanceOf[Column[Double]]
            rdd = col.rdd.map { cell => if (cell.isNaN) value else cell }.asInstanceOf[RDD[T]]
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
            val col = this.asInstanceOf[Column[String]]
            rdd = col.rdd.map { cell => if (cell.isEmpty) value else cell }.asInstanceOf[RDD[T]]
        } else {
            println("This is not a String column")
        }
    }

    private case object ColumnOfDoublesOps {
        def withColumnOfDoubles(a: Column[Double], b: Column[Double], oper: (Double, Double) => Double) = {
            val zipped = a.rdd.zip(b.rdd)
            val result = zipped.map { x => oper(x._1, x._2) }
            Column(result)
        }

        def filterDouble(a: Column[Double], b: Double, oper: (Double, Double) => Boolean) = {
            val result = a.rdd.filter { x => oper(x, b) }
            Column(result)
        }

        def withColumnOfString(a: Column[Double], b: Column[String], oper: (Double, String) => String) = {
            val zipped = a.rdd.zip(b.rdd)
            val result = zipped.map { x => oper(x._1, x._2) }
            Column(result)
        }

        def withScalarDouble(a: Column[Double], b: Double, oper: (Double, Double) => Double) = {
            val result = a.rdd.map { x => oper(x, b) }
            Column(result)
        }

        def withScalarString(a: Column[Double], b: String, oper: (Double, String) => String) = {
            val result = a.rdd.map { x => oper(x, b) }
            Column(result)
        }
    }

    private case object ColumnOfStringsOps {
        def withColumnOfDoubles(a: Column[String], b: Column[Double], oper: (String, Double) => String) = {
            val zipped = a.rdd.zip(b.rdd)
            val result = zipped.map { x => oper(x._1, x._2) }
            Column(result)
        }

        def filterDouble(a: Column[String], b: Double, oper: (String, Double) => Boolean) = {
            val result = a.rdd.filter { x => oper(x, b) }
            Column(result)
        }

        def withColumnOfString(a: Column[String], b: Column[String], oper: (String, String) => String) = {
            val zipped = a.rdd.zip(b.rdd)
            val result = zipped.map { x => oper(x._1, x._2) }
            Column(result)
        }

        def withScalarDouble(a: Column[String], b: Double, oper: (String, Double) => String) = {
            val result = a.rdd.map { x => oper(x, b) }
            Column(result)
        }

        def withScalarString(a: Column[String], b: String, oper: (String, String) => String) = {
            val result = a.rdd.map { x => oper(x, b) }
            Column(result)
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
     *  divide a column by another
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
    def map[U: ClassTag](mapper: T => U): Column[Any] = {
        val mapped = rdd.map { row => mapper(row) }
        if (classTag[U] == classTag[Double])
            Column(mapped.asInstanceOf[RDD[Double]]).asInstanceOf[Column[Any]]
        else
            Column(mapped.asInstanceOf[RDD[String]]).asInstanceOf[Column[Any]]
    }

}

/*
 * operations with a double as first param
 */
case object DoubleOps {
    private def colBool(bool: Boolean) = {
        if (bool) 1.0 else 0.0
    }
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
    private def colBool(bool: Boolean) = {
        if (bool) "t" else "f"
    }
    def addDouble(a: String, b: Double) = a + b
    def multiply(a: String, b: Double) = a * b.toInt

    def addString(a: String, b: String) = a + b

    def gt(a: String, b: String) = colBool(a > b)
    def gte(a: String, b: String) = colBool(a >= b)
    def lt(a: String, b: String) = colBool(a < b)
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
    def asDoubles(sc: SparkContext, stringRdd: RDD[String], index: Int, cacheLevel: StorageLevel) = {
        val parseErrors = sc.accumulator(0L)
        val doubleRdd = stringRdd map { x =>
            var y = Double.NaN
            try {
                y = x.toDouble
            } catch {
                case _: java.lang.NumberFormatException => parseErrors += 1
            }
            y
        }
        doubleRdd.setName(s"${stringRdd.name}/double").persist(cacheLevel)
        doubleRdd.foreach { x: Double => {} } //to trigger accumulation of parseErrors
        new Column[Double](doubleRdd, index, parseErrors.value)
    }

    def asDoubles(sc: SparkContext, col: Column[String], index: Int, cacheLevel: StorageLevel): Column[Double] = {
    	asDoubles(sc, col.rdd, index, cacheLevel)
    }

    /**
     * create Column from existing RDD
     */
    def apply[T: ru.TypeTag](rdd: RDD[T], index: Int = -1) = {
        val tpe = ru.typeOf[T]
        println(tpe)
        if (tpe =:= ru.typeOf[Double])
            newDoubleColumn(rdd.asInstanceOf[RDD[Double]], index)
        else if (tpe =:= ru.typeOf[String])
            newStringColumn(rdd.asInstanceOf[RDD[String]], index)
        else null
    }
    def newDoubleColumn(rdd: RDD[Double], index: Int) = {
        new Column(rdd, index, 0)
    }
    def newStringColumn(rdd: RDD[String], index: Int) = {
        new Column(rdd, index, 0)
    }
}
