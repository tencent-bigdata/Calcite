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
package org.apache.calcite.sql;

import java.util.List;
/**
 * SqlExtractWatermarkOP represents a relational set theory operator (UNION, INTERSECT,
 * MINUS). These are binary operators, but with an extra UDF.
 */
public class SqlExtractWatermarkOP extends SqlSetOperator {
  //~ Instance fields --------------------------------------------------------

  private SqlOperator operator;
  private final SqlIdentifier udfName;

  //~ Constructors -----------------------------------------------------------

  public SqlExtractWatermarkOP(SqlOperator setOperator, SqlIdentifier udfName) {
    super(setOperator.getName(), setOperator.getKind(),
            setOperator.getLeftPrec(), ((SqlSetOperator) setOperator).isAll());
    this.operator = setOperator;
    this.udfName = udfName;
  }

  //~ Methods ----------------------------------------------------------------
  public SqlOperator getSetOperator() {
    return operator;
  }
  public void setSqlSetOperator(SqlOperator op) {
    this.operator = op;
  }
  public List<String> udfNameInString() {
    return udfName.names;
  }
  public SqlIdentifier udfNameInSqlIdentifier() {
    return udfName;
  }
  public boolean withExtractwatermark() {
    return true;
  }
}

// End SqlExtractWatermarkOP.java
