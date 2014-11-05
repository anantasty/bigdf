/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.rdd.RDD

private[bigdf] object CountHelper {
    def countNaN(row: Array[Any]) = {
        var ret = 0
        for (col <- row) {
            val a = col match {
                case x: Double => x.isNaN
                case x: String => x.isEmpty
            }
            if (a == true) ret += 1
        }
        ret
    }
}

/*
 * needed this to work around task serialization failure in spark
 */
private[bigdf] case class PivotHelper(grped: RDD[(Any, Iterable[Array[Any]])],
                       pivotIndex: Int,
                       pivotValue: String) {
    def get = {
        grped.map {
            case (k, v) =>
                val vv = v.filter { row =>
                    row(pivotIndex) match {
                        case cellD: Double => cellD.toString == pivotValue
                        case cellS: String => cellS == pivotValue
                    }
                }
                (k, vv)
        }

    }
}

/**
 * Extend this class to do aggregations. Implement aggregate method.
 * Optionally override convert and finalize
 * @tparam U
 * @tparam V
 */
abstract case class Aggregator[U, V] {
    var colIndex: Int = -1
    def convert(a: Array[Any]): U = {
        a(colIndex).asInstanceOf[U]
    }
    def mergeValue(a: U, b: Array[Any]): U = {
        aggregate(a, convert(b))
    }
    def mergeCombiners(x: U, y: U): U = {
        aggregate(x, y)
    }

    /*
     * user supplied aggregator
     */
    def aggregate(p: U, q: U): U

    def finalize(x: U): V = x.asInstanceOf[V]
}

private[bigdf] object ColumnZipper {
  /**
   * zip columns to get rows as arrays
   * @param cols
   * @return RDD of columns zipped into Arrays
   */
  def apply(cols: Seq[Column[Any]]): RDD[Array[Any]] = {
    val first = cols.head.rdd
    val rest = cols.tail.map { _.rdd }

    //if you get a compile error here, you have the wrong spark
    //get my forked version or patch yours from my pull request
    //https://github.com/apache/spark/pull/2429
    first.zip(rest)
  }

  /**
   * zip columns to get rows as arrays
   * @param df
   * @param indices
   * @return RDD of columns zipped into Arrays
   */
   def apply(df: DF, indices: Seq[Int]): RDD[Array[Any]] = {
    val cols = indices.map{ colIndex => df.cols(df.colIndexToName(colIndex))}
    apply(cols)
  }

  /**
   *  zip columns to get rows as lists
   * @param df
   * @param indices
   * @return
   */
  def zip(df: DF, indices: Seq[Int]) = {
    val arrays = apply(df, indices)
    arrays.map{ _.toList }
  }

}
