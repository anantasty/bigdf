/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.rdd.RDD

object DFUtils {
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

case class PivotHelper(grped: RDD[(Any, Iterable[Array[Any]])],
                       pivotIndex: Int,
                       pivotValue: Double) {
    def get = {
        grped.map {
            case (k, v) =>
                (k, v.filter { row => row(pivotIndex) == pivotValue })
        }
    }
}

abstract case class Aggregator[U] {
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
}