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

package org.apache.spark.sql.execution.wasm

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionSet, WasmUDF}
import org.apache.spark.sql.catalyst.plans.logical.{BatchEvalWasm, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.WASM_UDF

object ExtractWasmUDFs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // SPARK-26293: A subquery will be rewritten into join later, and will go through this rule
    // eventually. Here we skip subquery, as Python UDF only needs to be extracted once.
    case s: Subquery if s.correlated => plan

    case _ => plan.transformUpWithPruning(
      _.containsPattern(WASM_UDF)) {
      case p: BatchEvalWasm => p

      case plan: LogicalPlan => extract(plan)
    }
  }

  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = ExpressionSet(collectEvaluableUDFsFromExpressions(plan.expressions))
      // ignore the PythonUDF that come from second/third aggregate, which is not used
      .filter(udf => udf.references.subsetOf(plan.inputSet))
      .toSeq.asInstanceOf[Seq[WasmUDF]]
    if (udfs.isEmpty) {
      // If there aren't any, we are done.
      plan
    } else {
      val attributeMap = mutable.HashMap[WasmUDF, Expression]()
      // Rewrite the child that has the input required for the UDF
      val newChildren = plan.children.map { child =>
        // Pick the UDF we are going to evaluate
        val validUdfs = udfs.filter { udf =>
          // Check to make sure that the UDF can be evaluated with only the input of this child.
          udf.references.subsetOf(child.outputSet)
        }
        if (validUdfs.nonEmpty) {
          val resultAttrs = validUdfs.zipWithIndex.map { case (u, i) =>
            AttributeReference(s"wasmUDF$i", u.dataType)()
          }

          attributeMap ++= validUdfs.map(canonicalizeDeterministic).zip(resultAttrs)
          BatchEvalWasm(validUdfs, resultAttrs, child)
        } else {
          child
        }
      }
      // Other cases are disallowed as they are ambiguous or would require a cartesian
      // product.
      udfs.map(canonicalizeDeterministic).filterNot(attributeMap.contains).foreach { udf =>
        throw new IllegalStateException(
          s"Invalid PythonUDF $udf, requires attributes from more than one child.")
      }

      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case p: WasmUDF => attributeMap.getOrElse(canonicalizeDeterministic(p), p)
      }

      // extract remaining python UDFs recursively
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        // Trim away the new UDF value if it was only used for filtering or something.
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }

  private def collectEvaluableUDFsFromExpressions(expressions: Seq[Expression]): Seq[WasmUDF] = {
    def collectUDFs(expr: Expression): Seq[WasmUDF] = expr match {
      case udf: WasmUDF =>
        Seq(udf)
      case e => e.children.flatMap(collectUDFs)
    }

    expressions.flatMap(collectUDFs)
  }

  private def canonicalizeDeterministic(u: WasmUDF) = {
    if (u.deterministic) {
      u.canonicalized.asInstanceOf[WasmUDF]
    } else {
      u
    }
  }

}
