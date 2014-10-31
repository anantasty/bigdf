/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */
package com.ayasdi.bigdf

import scala.reflect.runtime.{universe => ru}

abstract class Condition {
    def checkWithRowStrategy(row: Array[Any]): Boolean
    def checkWithColStrategy(cols: Array[Any]): Boolean
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
    def checkWithRowStrategy(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[Double])

    def checkWithColStrategy(cols: Array[Any]) = {
      println("DCS" + cols(0) + " " + cols(1))
      val index = colSeq.indexOf(colIndex)
      cmp(cols(index).asInstanceOf[Double])
    }
}

case class DoubleColumnWithDoubleColumnCondition(i: Int, j: Int,
                                                 val cmp: (Double, Double) => Boolean) extends BinaryCondition(i, j) {
    def checkWithRowStrategy(row: Array[Any]) = {
        val elemLeft = row(colIndexLeft).asInstanceOf[Double]
        val elemRight = row(colIndexRight).asInstanceOf[Double]
        cmp(elemLeft, elemRight)
    }

    def checkWithColStrategy(cols: Array[Any]) = {
      val elemLeft = cols(colSeq.indexOf(colIndexLeft)).asInstanceOf[Double]
      val elemRight = cols(colSeq.indexOf(colIndexRight)).asInstanceOf[Double]
      cmp(elemLeft, elemRight)
    }
}

case class StringColumnWithStringScalarCondition(i: Int,
                                                 val cmp: String => Boolean) extends UnaryCondition(i) {
    def checkWithRowStrategy(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[String])

    def checkWithColStrategy(cols: Array[Any]) = {
      val index = colSeq.indexOf(colIndex)
      cmp(cols(index).asInstanceOf[String])
    }
}

case class StringColumnWithStringColumnCondition(i: Int, j: Int,
                                                 val cmp: (String, String) => Boolean) extends BinaryCondition(i, j) {
    def checkWithRowStrategy(row: Array[Any]) = {
        val elemLeft = row(colIndexLeft).asInstanceOf[String]
        val elemRight = row(colIndexRight).asInstanceOf[String]
        cmp(elemLeft, elemRight)
    }

    def checkWithColStrategy(cols: Array[Any]) = {
      val elemLeft = cols(colSeq.indexOf(colIndexLeft)).asInstanceOf[String]
      val elemRight = cols(colSeq.indexOf(colIndexRight)).asInstanceOf[String]
      cmp(elemLeft, elemRight)
    }
}

case class AndCondition(l: Condition, r: Condition) extends CompoundCondition(l, r) {
    def checkWithRowStrategy(row: Array[Any]) = left.checkWithRowStrategy(row) && right.checkWithRowStrategy(row)
    def checkWithColStrategy(cols: Array[Any]) = {
      println("AND" + cols(0) + " " + cols(1))
      left.checkWithColStrategy(cols) && right.checkWithColStrategy(cols)
    }
}

case class OrCondition(l: Condition, r: Condition) extends CompoundCondition(l, r) {
    def checkWithRowStrategy(row: Array[Any]) = left.checkWithRowStrategy(row) || right.checkWithRowStrategy(row)
    def checkWithColStrategy(cols: Array[Any]) = left.checkWithColStrategy(cols) || right.checkWithColStrategy(cols)
}

case class XorCondition(l: Condition, r: Condition) extends CompoundCondition(l, r) {
    def checkWithRowStrategy(row: Array[Any]) = {
        val a = left.checkWithRowStrategy(row)
        val b = right.checkWithRowStrategy(row)

        (a || b) && !(a && b)
    }

    def checkWithColStrategy(cols: Array[Any]) = {
      val a = left.checkWithColStrategy(cols)
      val b = right.checkWithColStrategy(cols)

      (a || b) && !(a && b)
    }
}

case class NotCondition(val cond: Condition) extends Condition {
    def checkWithRowStrategy(row: Array[Any]) = !cond.checkWithRowStrategy(row)
    def checkWithColStrategy(row: Array[Any]) = !cond.checkWithColStrategy(row)
    def colSeq = cond.colSeq
}

case class DoubleColumnCondition(i: Int,
                                 val f: Double => Boolean) extends UnaryCondition(i) {
    def checkWithRowStrategy(row: Array[Any]) = f(row(colIndex).asInstanceOf[Double])
    def checkWithColStrategy(cols: Array[Any]) = f(cols(colSeq.indexOf(colIndex)).asInstanceOf[Double])
}

case class StringColumnCondition(i: Int,
                                 val f: String => Boolean) extends UnaryCondition(i) {
    def checkWithRowStrategy(row: Array[Any]) = f(row(colIndex).asInstanceOf[String])
    def checkWithColStrategy(cols: Array[Any]) = f(cols(colSeq.indexOf(colIndex)).asInstanceOf[String])
}



