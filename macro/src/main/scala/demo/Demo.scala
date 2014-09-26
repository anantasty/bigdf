package demo

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.annotation.StaticAnnotation
import scala.reflect.runtime.{universe => ru}

object Demo {
  
  def printer[T](a: T): Unit = macro printerImpl[T]
  
  def printerImpl[T: c.WeakTypeTag](c: Context)(a: c.Expr[T]): c.Expr[Unit] = {
      import c.universe._
      val tpe: Type = c.weakTypeOf[T]
      val result: Tree = if(tpe == c.weakTypeOf[Double])
          q"""println("Double:" + ${a.tree})"""
      else
          q"""println("Not Double:" + ${a.tree})"""
      c.Expr[Unit](result)
  }
   
//  def doubleFuncs: Unit = macro doubleFuncsImpl
//  def doubleFuncsImpl(c: Context): c.Expr[Unit] = {
//      import c.universe._
//      val tpe = c.weakTypeOf[Double]
//      tpe.members match {
//          case q"def +(x: $arg): Double" => println("!!!" + arg)
//          case _ => println("---")
//      }
//  }
  
}
