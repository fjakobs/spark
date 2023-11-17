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

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, MutableProjection, NamedArgumentExpression, UnsafeProjection, UnsafeRow, WasmUDF}
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType}



class BatchEvalWasmEvaluatorFactory(
     childOutput: Seq[Attribute],
     udfs: Seq[WasmUDF],
     output: Seq[Attribute],
     jobArtifactUUID: Option[String])
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new EvalWasmPartitionEvaluator

  def evaluate(
                // funcs: Seq[ChainedPythonFunctions],
                argMetas: Array[Array[ArgumentMetadata]],
                iter: Iterator[InternalRow],
                schema: StructType,
                context: TaskContext): Iterator[InternalRow] = {

    val inputIterator = BatchEvalWasmExec.getInputIterator(iter, schema)
//
    inputIterator.map { row =>
      val result = new GenericInternalRow(1)
      // val r = (row.asInstanceOf[Array[Array[Long]]])(0)
      val r = row(0)

      result.setLong(0,
        r(0).longValue() * r(1).longValue()
      )
      result
    }
  }

  private class EvalWasmPartitionEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    private def collectFunctions(udf: WasmUDF): Seq[Expression] = {

      udf.children match {
        case Seq(u: WasmUDF) =>
          val children = collectFunctions(u)
          children
        case children =>
          // There should not be any other UDFs, or the children can't be evaluated directly.
          assert(children.forall(!_.exists(_.isInstanceOf[WasmUDF])))
          udf.children
      }
    }
    def eval(
        partitionIndex: Int,
        iters: Iterator[InternalRow]*): Iterator[InternalRow] = {

      val iter = iters.head
      val context = TaskContext.get()

      val inputs = udfs.map(collectFunctions)

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argMetas = inputs.map { input =>
        input.map { e =>
          val (key, value) = e match {
            case NamedArgumentExpression(key, value) =>
              (Some(key), value)
            case _ =>
              (None, e)
          }
          if (allInputs.exists(_.semanticEquals(value))) {
            ArgumentMetadata(allInputs.indexWhere(_.semanticEquals(value)), key)
          } else {
            allInputs += value
            dataTypes += value.dataType
            ArgumentMetadata(allInputs.length - 1, key)
          }
        }.toArray
      }.toArray

      val projection = MutableProjection.create(allInputs.toSeq, childOutput)
      projection.initialize(context.partitionId())

      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }.toArray)

      val queue = mutable.Queue[InternalRow]()

      // Add rows to queue to join later with the result.
      val projectedRowIter = iter.map { inputRow =>
          queue.enqueue(inputRow.asInstanceOf[UnsafeRow])
          projection(inputRow)
        }

      var outputRowIterator =
        evaluate(argMetas, projectedRowIter, schema, context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      outputRowIterator = outputRowIterator.map { outputRow =>
        resultProj(joined(queue.dequeue(), outputRow))
      }

      outputRowIterator
    }
  }
}


