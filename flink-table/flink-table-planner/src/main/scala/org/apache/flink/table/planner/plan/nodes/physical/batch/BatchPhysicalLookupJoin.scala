/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLookupJoin
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin
import org.apache.flink.table.planner.plan.utils.{FlinkRexUtil, JoinTypeUtil}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rex.RexProgram

import java.util

import scala.collection.JavaConverters._

/** Batch physical RelNode for temporal table join that implements by lookup. */
class BatchPhysicalLookupJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    temporalTable: RelOptTable,
    tableCalcProgram: Option[RexProgram],
    joinInfo: JoinInfo,
    joinType: JoinRelType,
    lookupHint: Option[RelHint] = Option.empty,
    enableLookupShuffle: Boolean = false,
    preferCustomShuffle: Boolean = false)
  extends CommonPhysicalLookupJoin(
    cluster,
    traitSet,
    input,
    temporalTable,
    tableCalcProgram,
    joinInfo,
    joinType,
    lookupHint,
    false,
    enableLookupShuffle,
    preferCustomShuffle)
  with BatchPhysicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchPhysicalLookupJoin(
      cluster,
      traitSet,
      inputs.get(0),
      temporalTable,
      tableCalcProgram,
      joinInfo,
      joinType,
      lookupHint,
      enableLookupShuffle,
      preferCustomShuffle)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val (projectionOnTemporalTable, filterOnTemporalTable) = calcOnTemporalTable match {
      case Some(program) =>
        val (projection, filter) = FlinkRexUtil.expandRexProgram(program)
        (JavaScalaConversionUtil.toJava(projection), filter.orNull)
      case _ =>
        (null, null)
    }

    new BatchExecLookupJoin(
      tableConfig,
      JoinTypeUtil.getFlinkJoinType(joinType),
      finalPreFilterCondition.orNull,
      finalRemainingCondition.orNull,
      new TemporalTableSourceSpec(temporalTable),
      allLookupKeys.map(item => (Int.box(item._1), item._2)).asJava,
      projectionOnTemporalTable,
      filterOnTemporalTable,
      asyncOptions.orNull,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription,
      preferCustomShuffle)
  }
}
