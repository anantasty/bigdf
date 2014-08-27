/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.df

import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag.Double

import org.apache.spark.rdd.RDD

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
        var acc: RDD[List[Any]] = {
            cols(0)._2.rdd
     
        }.map { List(_) }
        for ((colName, col) <- cols.tail) {
            acc = 
                acc.zip(col.rdd).map { case (l, r) => r :: l }
        }
        acc.map { _.reverse.toArray }
    }
    
    def map[U: ClassTag](mapper: Array[Any] => U): Column[Any] = {
        val tpe = classTag[U]
        val partialRows = computePartialRows
        val mapped = partialRows.map { row => mapper(row) }
        if(tpe == classTag[Double])
        	new Column[Double](mapped.asInstanceOf[RDD[Double]], -1, 0).asInstanceOf[Column[Any]]
        else if(tpe == classTag[String])
            new Column[String](mapped.asInstanceOf[RDD[String]], -1, 0).asInstanceOf[Column[Any]]
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