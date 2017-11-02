/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.gpuenabler

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.gpuenabler.CUDAUtils
import org.apache.spark.util.Utils


import scala.collection.immutable.HashMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{TermSymbol, Type, typeOf}


import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}
// Some code taken from org.apache.spark.sql.catalyst.ScalaReflection

// Dummy object to make the code work with TRL samples program
private[gpuenabler] abstract class PartitionFormat
private[gpuenabler] case object ColumnFormat extends PartitionFormat


private[gpuenabler] object ColumnPartitionSchema {

  // Since we are creating a runtime mirror using the class loader of current thread,
  // we need to use def at here. So, every time we call mirror, it is using the
  // class loader of the current thread.
  // TODO check out if synchronization etc. is needed - see bug
  // https://issues.scala-lang.org/browse/SI-6240 about reflection not being thread-safe before 2.11
  // http://docs.scala-lang.org/overviews/reflection/thread-safety.html
  private[gpuenabler] val mirror: universe.Mirror =
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)

  var onlyLoadableClassesSupported: Boolean = false

  def schemaFor[T: ClassTag]: ColumnPartitionSchema = {

    def columnsFor(tpe: Type): IndexedSeq[ColumnSchema] = {

      tpe match {
        // 8-bit signed BE
        case t if t <:< typeOf[Byte] => Vector(new ColumnSchema(BYTE_COLUMN))
        // 16-bit signed BE
        case t if t <:< typeOf[Short] => Vector(new ColumnSchema(SHORT_COLUMN))
        // 32-bit signed BE
        case t if t <:< typeOf[Int] => Vector(new ColumnSchema(INT_COLUMN))
        // 64-bit signed BE
        case t if t <:< typeOf[Long] => Vector(new ColumnSchema(LONG_COLUMN))
        // 32-bit single-precision IEEE 754 floating point
        case t if t <:< typeOf[Float] => Vector(new ColumnSchema(FLOAT_COLUMN))
        // 64-bit double-precision IEEE 754 floating point
        case t if t <:< typeOf[Double] => Vector(new ColumnSchema(DOUBLE_COLUMN))
        // array of 8-bit signed BE
        case t if t <:< typeOf[Array[Byte]] => Vector(new ColumnSchema(BYTE_ARRAY_COLUMN))
        // array of 16-bit signed BE
        case t if t <:< typeOf[Array[Short]] => Vector(new ColumnSchema(SHORT_ARRAY_COLUMN))
        // array of 32-bit signed BE
        case t if t <:< typeOf[Array[Int]] => Vector(new ColumnSchema(INT_ARRAY_COLUMN))
        // array of 64-bit signed BE
        case t if t <:< typeOf[Array[Long]] => Vector(new ColumnSchema(LONG_ARRAY_COLUMN))
        // array of 32-bit single-precision IEEE 754 floating point
        case t if t <:< typeOf[Array[Float]] => Vector(new ColumnSchema(FLOAT_ARRAY_COLUMN))
        // array of 64-bit double-precision IEEE 754 floating point
        case t if t <:< typeOf[Array[Double]] => Vector(new ColumnSchema(DOUBLE_ARRAY_COLUMN))
        // TODO boolean - note that in JVM specification it does not have specified size
        // TODO char - note that it's different that C char*, it's more like short*?
        // TODO string - along with special storage space in separate place - it's enough to point
        // offset of start of current string in some big blob with concatenated strings
        // TODO option
        // TODO protection from cycles
        // TODO caching schemas for classes
        // TODO make it work with nested classes
        // TODO objects that contains null object property
        // TODO objects with internal objects without vals/vars will end up with null fields here
        // Generic object
        case t if !onlyLoadableClassesSupported ||
            CUDAUtils.sparkUtils.classIsLoadable(t.typeSymbol.asClass.fullName) => {
          val valVarMembers = t.members.view
            .filter(p => !p.isMethod && p.isTerm).map(_.asTerm)
            .filter(p => p.isVar || p.isVal)

          valVarMembers.foreach { p =>
            // TODO more checks
            // is final okay?
            if (p.isStatic) throw new UnsupportedOperationException(
                s"Column schema with static field ${p.fullName} not supported")
          }

          valVarMembers.flatMap { term =>
            columnsFor(term.typeSignature).map { schema =>
              new ColumnSchema(
                schema.columnType,
                term +: schema.terms)
            }
          } .toIndexedSeq
        }
        case other =>
          throw new UnsupportedOperationException(s"Column schema for type $other not supported")
      }
    }

    val runtimeCls = implicitly[ClassTag[T]].runtimeClass
    val columns = runtimeCls match {
      // special case for primitives, since their type signature shows up as AnyVal instead
      case c if c == classOf[Byte] => Vector(new ColumnSchema(BYTE_COLUMN))
      case c if c == classOf[Short] => Vector(new ColumnSchema(SHORT_COLUMN))
      case c if c == classOf[Int] => Vector(new ColumnSchema(INT_COLUMN))
      case c if c == classOf[Long] => Vector(new ColumnSchema(LONG_COLUMN))
      case c if c == classOf[Float] => Vector(new ColumnSchema(FLOAT_COLUMN))
      case c if c == classOf[Double] => Vector(new ColumnSchema(DOUBLE_COLUMN))
      // special case for primitive arrays
      case c if c == classOf[Array[Byte]] => Vector(new ColumnSchema(BYTE_ARRAY_COLUMN))
      case c if c == classOf[Array[Short]] => Vector(new ColumnSchema(SHORT_ARRAY_COLUMN))
      case c if c == classOf[Array[Int]] => Vector(new ColumnSchema(INT_ARRAY_COLUMN))
      case c if c == classOf[Array[Long]] => Vector(new ColumnSchema(LONG_ARRAY_COLUMN))
      case c if c == classOf[Array[Float]] => Vector(new ColumnSchema(FLOAT_ARRAY_COLUMN))
      case c if c == classOf[Array[Double]] => Vector(new ColumnSchema(DOUBLE_ARRAY_COLUMN))
      // generic case for other objects
      case _ =>
        val clsSymbol = mirror.classSymbol(runtimeCls)
        columnsFor(clsSymbol.typeSignature)
    }

    new ColumnPartitionSchema(columns.toArray, runtimeCls)
  }

}

/**
 * A schema of a ColumnPartitionData. columns contains information about columns and cls is the
 * class of the serialized type, unless it is primitive - then it is null.
 */
private[gpuenabler] class ColumnPartitionSchema(
    private var _columns: Array[ColumnSchema],
    private var _cls: Class[_]) extends Serializable {

  def columns: Array[ColumnSchema] = _columns

  def cls: Class[_] = _cls

  /**
   * Whether the schema is for a primitive value.
   */
  def isPrimitive: Boolean = columns.size == 1 && columns(0).terms.isEmpty

  /**
   * Amount of bytes used for storage of specified number of records using this schema.
   */
  def memoryUsage(size: Long): Long = {
    columns.map(_.memoryUsage(size)).sum
  }

  /**
   * Returns column schemas ordered by given pretty accessor names.
   */
  def orderedColumns(order: Seq[String]): Seq[ColumnSchema] = {
    val columnsByAccessors = HashMap(columns.map(col => col.prettyAccessor -> col): _*)
    order.map(columnsByAccessors(_))
  }

  def getAllColumns(): Seq[String] = {
    columns.map(col => col.prettyAccessor)
  }

  def getters: Array[Any => Any] = {
    val mirror = ColumnPartitionSchema.mirror
    columns.map { col =>
      col.terms.foldLeft(identity[Any] _)((r, term) =>
          ((obj: Any) => mirror.reflect(obj).reflectField(term).get) compose r)
    }
  }

  // the first argument is object, the second is value
  def setters: Array[(Any, Any) => Unit] = {
    assert(!isPrimitive)
    val mirror = ColumnPartitionSchema.mirror
    columns.map { col =>
      val getOuter = col.terms.dropRight(1).foldLeft(identity[Any] _)((r, term) =>
          ((obj: Any) => mirror.reflect(obj).reflectField(term).get) compose r)

      (obj: Any, value: Any) =>
        mirror.reflect(getOuter(obj)).reflectField(col.terms.last).set(value)
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = CUDAUtils.sparkUtils.tryOrIOException {
    out.writeObject(_columns)
    if (!isPrimitive) {
      out.writeUTF(_cls.getName())
    }
  }

  private def readObject(in: ObjectInputStream): Unit = CUDAUtils.sparkUtils.tryOrIOException {
    _columns = in.readObject().asInstanceOf[Array[ColumnSchema]]
    if (!isPrimitive) {
      _cls = CUDAUtils.sparkUtils.classForName(in.readUTF())
    }
  }

}

/**
 * A column is one basic property (primitive, String, etc.).
 */
private[gpuenabler] class ColumnSchema(
    /** Type of the property. Is null when the whole object is a primitive. */
    private var _columnType: ColumnType,
    /** Scala terms with property name and other information */
    private var _terms: Vector[TermSymbol] = Vector[TermSymbol]()) extends Serializable {

  def columnType: ColumnType = _columnType

  def terms: Vector[TermSymbol] = _terms

  /**
   * Chain of properties accessed starting from the original object. The first tuple argument is
   * the full name of the class containing the property and the second is property's name.
   */
  def propertyChain: Vector[(String, String)] = {
    val mirror = ColumnPartitionSchema.mirror
    terms.map { term =>
      (mirror.runtimeClass(term.owner.asClass).getName, term.name.toString)
    }
  }

  def prettyAccessor: String = {
    // val mirror = ColumnPartitionSchema.mirror
    "this" + terms.map("." + _.name.toString.trim).mkString
  }

  def memoryUsage(size: Long): Long = {
    columnType.bytes * size
  }

  private def writeObject(out: ObjectOutputStream): Unit = CUDAUtils.sparkUtils.tryOrIOException {
    // TODO make it handle generic owner objects by passing full type information somehow
    out.writeObject(_columnType)
    out.writeObject(propertyChain)
  }

  private def readObject(in: ObjectInputStream): Unit = CUDAUtils.sparkUtils.tryOrIOException {
    val mirror = ColumnPartitionSchema.mirror
    _columnType = in.readObject().asInstanceOf[ColumnType]
    _terms =
      in.readObject().asInstanceOf[Vector[(String, String)]].map { case (clsName, propName) =>
        val cls = CUDAUtils.sparkUtils.classForName(clsName)
        val typeSig = mirror.classSymbol(cls).typeSignature
        // typeSig.declaration(universe.stringToTermName(propName)).asTerm // Scala_2.10
        typeSig.decl(universe.TermName(propName)).asTerm // Scala_2.11
      }
  }

}

private[gpuenabler] abstract class ColumnType {
  val bytes: Int            /* How many bytes does a single property take. */
  val elementLength: Int    /* length for each array element */
  def isArray: Boolean = (elementLength > 0)
}

private[gpuenabler] case object BYTE_COLUMN extends ColumnType {
  val bytes = 1
  val elementLength = 0
}

private[gpuenabler] case object SHORT_COLUMN extends ColumnType {
  val bytes = 2
  val elementLength = 0
}

private[gpuenabler] case object INT_COLUMN extends ColumnType {
  val bytes = 4
  val elementLength = 0
}

private[gpuenabler] case object LONG_COLUMN extends ColumnType {
  val bytes = 8
  val elementLength = 0
}

private[gpuenabler] case object FLOAT_COLUMN extends ColumnType {
  val bytes = 4
  val elementLength = 0
}

private[gpuenabler] case object DOUBLE_COLUMN extends ColumnType {
  val bytes = 8
  val elementLength = 0
}

private[gpuenabler] case object BYTE_ARRAY_COLUMN extends ColumnType {
  val bytes = 8         // size of offset in blob
  val elementLength = 1
}

private[gpuenabler] case object SHORT_ARRAY_COLUMN extends ColumnType {
  val bytes = 8         // size of offset in blob
  val elementLength = 2
}

private[gpuenabler] case object INT_ARRAY_COLUMN extends ColumnType {
  val bytes = 8         // size of offset in blob
  val elementLength = 4
}

private[gpuenabler] case object LONG_ARRAY_COLUMN extends ColumnType {
  val bytes = 8         // size of offset in blob
  val elementLength = 8
}

private[gpuenabler] case object FLOAT_ARRAY_COLUMN extends ColumnType {
  val bytes = 8         // size of offset in blob
  val elementLength = 4
}

private[gpuenabler] case object DOUBLE_ARRAY_COLUMN extends ColumnType {
  val bytes = 8         // size of offset in blob
  val elementLength = 8
}

