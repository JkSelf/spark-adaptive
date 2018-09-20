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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

case class OptimizeInitialPartitionNum(conf: SQLConf) extends Rule[SparkPlan] {

  private def autoCalculateInitialPartitionNum(totalInputSize: BigInt): Int = {
    val autoInitialPartitionsNum = Math.ceil(
      totalInputSize.toLong * 1.0 / conf.targetPostShuffleInputSize).toInt
    if (autoInitialPartitionsNum < conf.minNumPostShufflePartitions) {
      conf.minNumPostShufflePartitions
    } else if (autoInitialPartitionsNum > conf.maxNumPostShufflePartitions) {
      conf.maxNumPostShufflePartitions
    } else {
      autoInitialPartitionsNum
    }
  }

  private def optimizeInitialPartitionNum(
    plan: SparkPlan,
    reuseStages: mutable.HashMap[StructType, ArrayBuffer[ShuffleQueryStage]] = null): SparkPlan = {
    val oldPlan = plan
    val shuffleQueryStages: Seq[ShuffleQueryStage] = plan.collect {
      case input: ShuffleQueryStageInput => input.childStage
    }
    if (shuffleQueryStages.nonEmpty) {
      val initialPartitionNum = if (conf.adaptiveAutoCalculateInitialPartitionNum) {
        val totalInputSize = shuffleQueryStages.map {
          shuffleQueryStage: ShuffleQueryStage =>
            val child = shuffleQueryStage.child.asInstanceOf[ShuffleExchangeExec].child
            child.stats.sizeInBytes
        }.sum
        autoCalculateInitialPartitionNum(totalInputSize)
      } else {
        conf.maxNumPostShufflePartitions
      }

      if (reuseStages != null) {
        // "spark.sql.exchange.reuse" is true.There may exist duplicated exchanges
        shuffleQueryStages.foreach {
          shuffleQueryStage: ShuffleQueryStage =>
            val sameSchema = reuseStages.getOrElseUpdate(
              shuffleQueryStage.child.schema, ArrayBuffer[ShuffleQueryStage]())
            val samePlan = sameSchema.find { s => shuffleQueryStage.child.sameResult(s.child) }
            val child = shuffleQueryStage.child.asInstanceOf[ShuffleExchangeExec]
            if (samePlan.isDefined) {
              // the shuffleQueryStage is re-used
              child.newPartitioning = child.newPartitioning match {
                case hash: HashPartitioning =>
                  if (hash.numPartitions < initialPartitionNum) {
                    hash.copy(numPartitions = initialPartitionNum)
                  } else {
                    child.newPartitioning
                  }
                case collection: PartitioningCollection =>
                  collection.partitionings match {
                    case hash: HashPartitioning =>
                      if (hash.numPartitions < initialPartitionNum) {
                        hash.copy(numPartitions = initialPartitionNum)
                      } else {
                        child.newPartitioning
                      }
                  }
                case _ => child.newPartitioning
              }
            } else {
              // the shuffleQueryStage is not re-used
              child.newPartitioning = child.newPartitioning match {
                case hash: HashPartitioning =>
                  hash.copy(numPartitions = initialPartitionNum)
                case collection: PartitioningCollection =>
                  collection.partitionings match {
                    case hash: HashPartitioning =>
                      hash.copy(numPartitions = initialPartitionNum)
                  }
                case _ => child.newPartitioning
              }
              sameSchema += shuffleQueryStage
            }
        }
      } else {
        // "spark.sql.exchange.reuse" is false.
        shuffleQueryStages.foreach {
          shuffleQueryStage: ShuffleQueryStage =>
            val child = shuffleQueryStage.child.asInstanceOf[ShuffleExchangeExec]
            child.newPartitioning = child.newPartitioning match {
              case hash: HashPartitioning =>
                hash.copy(numPartitions = initialPartitionNum)
              case collection: PartitioningCollection =>
                collection.partitionings match {
                  case hash: HashPartitioning =>
                    hash.copy(numPartitions = initialPartitionNum)
                }
              case _ => child.newPartitioning
            }
        }
      }
      shuffleQueryStages.foreach(optimizeInitialPartitionNum(_, reuseStages))
    }
    oldPlan
  }

  private def adjustInitialPartitionNumSameStage(plan: SparkPlan): SparkPlan = {
    val oldPlan = plan
    val shuffleQueryStages: Seq[ShuffleQueryStage] = plan.collect {
      case input: ShuffleQueryStageInput => input.childStage
    }
    if (shuffleQueryStages.nonEmpty) {
      if (shuffleQueryStages.map(_.outputPartitioning.numPartitions).distinct.length != 1) {
        val max = shuffleQueryStages.map(_.outputPartitioning.numPartitions).max
        shuffleQueryStages.foreach {
          shuffleQueryStage: ShuffleQueryStage =>
            val child = shuffleQueryStage.child.asInstanceOf[ShuffleExchangeExec]
            child.newPartitioning = child.newPartitioning match {
              case hash: HashPartitioning =>
                hash.copy(numPartitions = max)
              case collection: PartitioningCollection =>
                collection.partitionings match {
                  case hash: HashPartitioning =>
                    hash.copy(numPartitions = max)
                }
              case _ => child.newPartitioning
            }
        }
      }
      shuffleQueryStages.foreach(adjustInitialPartitionNumSameStage(_))
    }
    oldPlan
  }

  def apply(plan: SparkPlan): SparkPlan = {

    val newPlan = if (!conf.exchangeReuseEnabled) {
      optimizeInitialPartitionNum(plan)
    } else {
      // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
      val stages = mutable.HashMap[StructType, ArrayBuffer[ShuffleQueryStage]]()
      optimizeInitialPartitionNum(plan, stages)
      // For the shuffleQueryStage reuse situation, we should adjust the initial partition
      // to be equal in same stage.
      adjustInitialPartitionNumSameStage(plan)
    }
    newPlan
  }
}