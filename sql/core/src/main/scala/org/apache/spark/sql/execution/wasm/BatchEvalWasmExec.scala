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

import org.apache.spark.JobArtifactSet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, WasmUDF}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.StructType

case class BatchEvalWasmExec(udfs: Seq[WasmUDF], resultAttrs: Seq[Attribute], child: SparkPlan)
  extends EvalWasmExec {

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override protected def evaluatorFactory: BatchEvalWasmEvaluatorFactory = {
    new BatchEvalWasmEvaluatorFactory(
      child.output,
      udfs,
      output,
      jobArtifactUUID)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BatchEvalWasmExec =
    copy(child = newChild)
}

trait EvalWasmExec extends UnaryExecNode {
  def udfs: Seq[WasmUDF]
  def resultAttrs: Seq[Attribute]

  protected def evaluatorFactory: BatchEvalWasmEvaluatorFactory

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        evaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }
}

object BatchEvalWasmExec {
  def getInputIterator(
      iter: Iterator[InternalRow],
      schema: StructType): Iterator[Array[Any]] = {

    val dataTypes = schema.map(_.dataType)

    iter.map { row =>
      val fields = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        val dt = dataTypes(i)
        fields(i) = row.get(i, dt)

        i += 1
      }
      fields
    }.grouped(100).map(_.toArray)
  }
}
