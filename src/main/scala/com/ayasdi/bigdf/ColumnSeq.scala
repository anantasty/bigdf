/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

/*
 * Column sequence
 */
case class ColumnSeq(val cols: Seq[(String, Column[Any])]) {
    def describe() {
        cols.foreach {
            case (name, col) =>
                println(name + ":")
                println(col.toString)
        }
    }
    
    private def computePartialRows: RDD[Array[Any]] = {
      val first = cols(0)._2.rdd
      val rest = cols.tail.map { _._2.rdd }

      first.zip(rest)
    }
    
    def map[U: ClassTag](mapper: Array[Any] => U): Column[Any] = {
        val tpe = classTag[U]
        val partialRows = computePartialRows
        val mapped = partialRows.map { row => mapper(row) }
        if(tpe == classTag[Double])
        	Column(mapped.asInstanceOf[RDD[Double]]).asInstanceOf[Column[Any]]
        else if(tpe == classTag[String])
            Column(mapped.asInstanceOf[RDD[String]]).asInstanceOf[Column[Any]]
        else
            null
    }

    override def toString() = {
        cols.map { x => 
            	"\n" + x._1 + ":\n" + x._2.toString 
            }
            .reduce { (strLeft, strRight) =>
                strLeft + strRight
            }
    }
}
