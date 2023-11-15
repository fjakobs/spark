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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, WASM_UDF}
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.types.DataType

trait WasmFuncExpression extends NonSQLExpression with UserDefinedExpression { self: Expression =>
  def name: String
  def bytecode: Array[Byte]
  def evalType: Int
  def udfDeterministic: Boolean
  def resultId: ExprId

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)
  override def toString: String = s"$name(${children.mkString(", ")})#${resultId.id}$typeSuffix"
  override def nullable: Boolean = true
}

case class WasmUDF(
    name: String,
    bytecode: Array[Byte],
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    resultId: ExprId = NamedExpression.newExprId)
  extends Expression with WasmFuncExpression with Unevaluable {

  lazy val resultAttribute: Attribute = AttributeReference(toPrettySQL(this), dataType, nullable)(
    exprId = resultId)

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(WASM_UDF)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): WasmUDF =
    copy(children = newChildren)
}