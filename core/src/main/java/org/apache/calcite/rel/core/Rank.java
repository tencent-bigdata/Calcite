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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * <code>Calc</code> is an abstract base class for implementations of
 * {@link org.apache.calcite.rel.logical.LogicalRank}.
 */
public abstract class Rank extends SingleRel {

  public Window window;

  protected final RexProgram program;

  private long topN;

  public Rank(RelOptCluster cluster,
              RelTraitSet traits,
              RelNode input,
              RexProgram program,
              Window window) {
    super(cluster, traits, input);
    this.window = window;
    this.program = program;
    this.rowType = window.getRowType();
  }

  @Override public final Rank copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), program, window);
  }

  public abstract Rank copy(
          RelTraitSet traitSet,
          RelNode input,
          RexProgram program,
          Window window);

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<Window.Group> window : Ord.zip(window.groups)) {
      pw.item("window#" + window.i, window.e.toString());
    }
    return pw;
  }

  @Override public boolean isValid(Litmus litmus, Context context) {
    return window.isValid(litmus, context);
  }

  public RexProgram getProgram() {
    return program;
  }

  public ImmutableList<RexNode> getFilters() {
    Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> project_filter = program.split();
    ImmutableList<RexNode> filters = project_filter.right;
    return filters;
  }

  public ImmutableList<RexNode> getProjects() {
    Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> project_filter = program.split();
    ImmutableList<RexNode> projects = project_filter.left;
    return projects;
  }

  public void extractTopN() {
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
          RexNode right = ((RexCall) filter).operands.get(1);
          if (left instanceof RexInputRef) {
            if (((RexInputRef) left).getIndex() == filed_i) {
              if (right instanceof RexLiteral) {
                topN = ((RexLiteral) right).getValueAs(Long.class);
              }
            }
          }
        }
      }
    }
  }
  public long getTopN() {
    return topN;
  }
}

// End Rank.java
