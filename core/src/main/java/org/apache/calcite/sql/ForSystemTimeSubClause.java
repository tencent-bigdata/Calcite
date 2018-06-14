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

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Locale;

/**
 * Enumerates the sub-clauses of FOR SYSTEM TIME.
 */
public enum ForSystemTimeSubClause {
  /**
   * FOR SYSTEM TIME AS OF <date_time> .
   */
  AS_OF,

  /**
   * FOR SYSTEM TIME FROM <start_date_time> TO <end_date_time>.
   */
  FROM_TO,

  /**
   * FOR SYSTEM TIME BETWEEN <start_date_time> AND <end_date_time>.
   **/
  BETWEEN_AND,

  /**
   *  FOR SYSTEM TIME CONTAINED IN(<start_date_time> ,<end_date_time>) .
   **/
  CONTAINED_IN;

  /** Lower-case name. */
  public final String lowerName = name().toLowerCase(Locale.ROOT);

  /**
   * Creates a parse-tree node representing an occurrence of this
   * sub-clause keyword at a particular position in the parsed
   * text.
   */
  public SqlLiteral symbol(SqlParserPos pos) {
    return SqlLiteral.createSymbol(this, pos);
  }
}
// End ForSystemTimeSubClause.java
