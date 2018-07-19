/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalRank;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlRankFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Planner rule that merges a
 * {@link org.apache.calcite.rel.logical.LogicalCalc} and a
 * {@link org.apache.calcite.rel.logical.LogicalWindow}. The
 * result is a {@link org.apache.calcite.rel.logical.LogicalRank}.
 */
public class CalcWindowMergeRule extends RelOptRule {

  private static final Predicate<Calc> PREDICATE_TOPN =
      new PredicateImpl<Calc>() {
    public boolean test(Calc calc) {
      boolean isTopN = false;
      if ((calc.getInput() instanceof HepRelVertex) && calcWithTopN(calc.getProgram())) {
        if (((HepRelVertex) calc.getInput()).getCurrentRel() instanceof LogicalWindow) {
          for (Window.Group group
                  : ((LogicalWindow) ((HepRelVertex) calc.getInput()).getCurrentRel()).groups) {
            for (Window.RexWinAggCall aggCall : group.aggCalls) {
              if (aggCall.getOperator() instanceof SqlRankFunction) {
                isTopN = true;
                break;
              }
            }
          }
        }
      }
      if ((calc.getInput() instanceof RelSubset) && calcWithTopN(calc.getProgram())) {
        if (((RelSubset) calc.getInput()).getOriginal() instanceof LogicalWindow) {
          for (Window.Group group
                  : ((LogicalWindow) ((RelSubset) calc.getInput()).getOriginal()).groups) {
            for (Window.RexWinAggCall aggCall : group.aggCalls) {
              if (aggCall.getOperator() instanceof SqlRankFunction) {
                isTopN = true;
                break;
              }
            }
          }
        }
      }
      return isTopN;
    }
        public boolean calcWithTopN(RexProgram program) {
          Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> project_filter = program.split();
          ImmutableList<RexNode> filters = project_filter.right;
          List<RelDataTypeField> fields = program.getInputRowType().getFieldList();
          int filed_i = 0;
          for (RelDataTypeField field : fields) {
            String fieldName = field.getName();
            if (fieldName.startsWith("w") && fieldName.contains("$o")) {
              break;
            }
            filed_i++;
          }
          for (RexNode filter : filters) {
            if (filter instanceof RexCall) {
              if (((RexCall) filter).op == SqlStdOperatorTable.LESS_THAN_OR_EQUAL) {
                RexNode left = ((RexCall) filter).operands.get(0);
                if (left instanceof RexInputRef) {
                  if (((RexInputRef) left).getIndex() == filed_i) {
                    return true;
                  }
                }
              }
            }
          }
          return false;
        }
  };
  public static final CalcWindowMergeRule INSTANCE =
          new CalcWindowMergeRule(RelFactories.LOGICAL_BUILDER);

  public CalcWindowMergeRule(RelBuilderFactory relBuilderFactory) {
    super(
            operand(
            LogicalCalc.class,
            null,
            PREDICATE_TOPN,
            operand(LogicalWindow.class, any())),
            relBuilderFactory, null);
  }

  public void onMatch(RelOptRuleCall call) {
    final LogicalCalc calc = call.rel(0);
    final LogicalWindow window = call.rel(1);
    final RelBuilder relBuilder = call.builder();
    LogicalRank rank = LogicalRank.create(window.getTraitSet(), window.getInput(), calc
            .getProgram(), window);
    call.transformTo(relBuilder.push(rank).
            calcForTopN(calc.getProgram(), calc.getCluster().getRexBuilder()).build());
  }
}

// End CalcWindowMergeRule.java
