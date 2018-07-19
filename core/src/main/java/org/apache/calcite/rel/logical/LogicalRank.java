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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Rank;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexProgram;


/**
 * <p>This relational expression combines the functionality of
 * {@link LogicalCalc} and {@link LogicalWindow}.
 * It should be created in the later
 * stages of optimization, by merging consecutive {@link LogicalCalc} and
 * {@link LogicalWindow} nodes together.
 *
 */
public final class LogicalRank extends Rank {

  /**Create a LogicalRank*/
  public LogicalRank(RelOptCluster cluster,
                     RelTraitSet traitSet,
                     RelNode input,
                     RexProgram program,
                     Window window) {
    super(cluster, traitSet, input, program, window);
    extractTopN();
  }

  @Override public LogicalRank copy(RelTraitSet traitSet,
                                    RelNode input,
                                    RexProgram program,
                                    Window window) {
    return new LogicalRank(getCluster(), traitSet, input, program, window);
  }

  public static LogicalRank create(RelTraitSet traitSet,
                                   RelNode input,
                                   RexProgram program,
                                   LogicalWindow window) {
    return new LogicalRank(input.getCluster(), traitSet, input, program, window);
  }
}

// End LogicalRank.java
