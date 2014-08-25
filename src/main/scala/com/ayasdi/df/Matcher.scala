/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.df

import scala.reflect.runtime.{ universe => ru }

abstract class Condition {
    def check(row: Array[Any]): Boolean

    def &&(that: Condition) = {
        new AndCondition(this, that)
    }
    def &(that: Condition) = &&(that)

    def ||(that: Condition) = {
        new OrCondition(this, that)
    }
    def |(that: Condition) = ||(that)

    def ^^(that: Condition) = {
        new XorCondition(this, that)
    }

    def unary_! = {
        new NotCondition(this)
    }
    def unary_~ = unary_!
}

case class DoubleColumnWithDoubleScalarCondition(val colIndex: Int,
                                                 val cmp: Double => Boolean) extends Condition {
    def check(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[Double])
}

case class DoubleColumnWithDoubleColumnCondition(val colIndexLeft: Int,
                                                 val colIndexRight: Int,
                                                 val cmp: (Double, Double) => Boolean) extends Condition {
    def check(row: Array[Any]) = {
        val elemLeft = row(colIndexLeft)
        val elemRight = row(colIndexRight)
        (elemLeft, elemRight) match {
            case (l: Double, r: Double) => cmp(l, r)
            case _                      => println(s"DoubleColumnCondition2 only likes Doubles"); false
        }
    }
}

case class StringColumnWithStringScalarCondition(val colIndex: Int,
                                                 val cmp: String => Boolean) extends Condition {
    def check(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[String])
}

case class StringColumnWithStringColumnCondition(val colIndexLeft: Int,
                                                 val colIndexRight: Int,
                                                 val cmp: (String, String) => Boolean) extends Condition {
    def check(row: Array[Any]) = {
        val elemLeft = row(colIndexLeft)
        val elemRight = row(colIndexRight)
        (elemLeft, elemRight) match {
            case (l: String, r: String) => cmp(l, r)
            case _                      => println(s"DoubleColumnCondition2 only likes Doubles"); false
        }
    }
}

case class AndCondition(val left: Condition,
                        val right: Condition) extends Condition {
    def check(row: Array[Any]) = left.check(row) && right.check(row)
}

case class OrCondition(val left: Condition,
                       val right: Condition) extends Condition {
    def check(row: Array[Any]) = left.check(row) || right.check(row)
}

case class XorCondition(val left: Condition,
                        val right: Condition) extends Condition {
    def check(row: Array[Any]) = {
        val a = left.check(row)
        val b = right.check(row)

        (a || b) && !(a && b)
    }
}

case class NotCondition(val cond: Condition) extends Condition {
    def check(row: Array[Any]) = !cond.check(row)
}

case class DoubleColumnCondition(val colIndex: Int,
                                 val f: Double => Boolean) extends Condition {
    def check(row: Array[Any]) = f(row(colIndex).asInstanceOf[Double])
}

case class StringColumnCondition(val colIndex: Int,
                                 val f: String => Boolean) extends Condition {
    def check(row: Array[Any]) = f(row(colIndex).asInstanceOf[String])
}


