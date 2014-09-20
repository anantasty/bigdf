/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.bigdf

import scala.reflect.runtime.{ universe => ru }

abstract class Condition {
    def check(row: Array[Any]): Boolean
    def colSeq: List[Int]

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

abstract class UnaryCondition(val colIndex: Int) extends Condition with Serializable {
    def colSeq = List(colIndex)
}

abstract class BinaryCondition(val colIndexLeft: Int,
                               val colIndexRight: Int) extends Condition with Serializable {
    def colSeq = List(colIndexLeft, colIndexRight)
}

abstract class CompoundCondition(val left: Condition,
                                 val right: Condition) extends Condition with Serializable {
    def colSeq = left.colSeq ++ right.colSeq
}

case class DoubleColumnWithDoubleScalarCondition(i: Int, val cmp: Double => Boolean) extends UnaryCondition(i) {
    def check(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[Double])
}

case class DoubleColumnWithDoubleColumnCondition(i: Int, j: Int,
                                                 val cmp: (Double, Double) => Boolean) extends BinaryCondition(i, j) {
    def check(row: Array[Any]) = {
        val elemLeft = row(colIndexLeft)
        val elemRight = row(colIndexRight)
        (elemLeft, elemRight) match {
            case (l: Double, r: Double) => cmp(l, r)
            case _                      => println(s"DoubleColumnCondition2 only likes Doubles"); false
        }
    }

}

case class StringColumnWithStringScalarCondition(i: Int,
                                                 val cmp: String => Boolean) extends UnaryCondition(i) {
    def check(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[String])
}

case class StringColumnWithStringColumnCondition(i: Int, j: Int,
                                                 val cmp: (String, String) => Boolean) extends BinaryCondition(i, j) {
    def check(row: Array[Any]) = {
        val elemLeft = row(colIndexLeft)
        val elemRight = row(colIndexRight)
        (elemLeft, elemRight) match {
            case (l: String, r: String) => cmp(l, r)
            case _                      => println(s"DoubleColumnCondition2 only likes Doubles"); false
        }
    }
}

case class AndCondition(l: Condition, r: Condition) extends CompoundCondition(l, r) {
    def check(row: Array[Any]) = left.check(row) && right.check(row)
}

case class OrCondition(l: Condition, r: Condition) extends CompoundCondition(l, r) {
    def check(row: Array[Any]) = left.check(row) || right.check(row)
}

case class XorCondition(l: Condition, r: Condition) extends CompoundCondition(l, r) {
    def check(row: Array[Any]) = {
        val a = left.check(row)
        val b = right.check(row)

        (a || b) && !(a && b)
    }
}

case class NotCondition(val cond: Condition) extends Condition {
    def check(row: Array[Any]) = !cond.check(row)
    def colSeq = cond.colSeq
}

case class DoubleColumnCondition(i: Int,
                                 val f: Double => Boolean) extends UnaryCondition(i) {
    def check(row: Array[Any]) = f(row(colIndex).asInstanceOf[Double])
}

case class StringColumnCondition(i: Int,
                                 val f: String => Boolean) extends UnaryCondition(i) {
    def check(row: Array[Any]) = f(row(colIndex).asInstanceOf[String])
}


