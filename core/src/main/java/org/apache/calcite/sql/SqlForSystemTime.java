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
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree node representing a {@code FOR SYSTEM TIME} clause.
 */
public class SqlForSystemTime extends SqlCall {
  public static final SqlForSystemTimeOperator OPERATOR = new SqlForSystemTimeOperator();
  /**
   * Temporal Table*/
  SqlNode table;

  SqlLiteral subclause;

  SqlNode startDateTime;

  SqlNode endDateTime;

  //~ Constructors -----------------------------------------------------------
  public SqlForSystemTime(SqlParserPos pos, SqlNode table, SqlLiteral subclause,
                          SqlNode startDateTime, SqlNode endDateTime) {
    super(pos);
    this.table = table;
    this.subclause = Objects.requireNonNull(subclause);
    this.startDateTime = Objects.requireNonNull(startDateTime);
    this.endDateTime = endDateTime;

    Objects.requireNonNull(subclause.symbolValue(ForSystemTimeSubClause.class));
  }

  //~ Methods ----------------------------------------------------------------
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public SqlKind getKind() {
    return SqlKind.FOR_SYSTEM_TIME;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(table, subclause, startDateTime, endDateTime);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      table = operand;
      break;
    case 1:
      subclause = (SqlLiteral) operand;
      break;
    case 2:
      startDateTime = operand;
      break;
    case 3:
      endDateTime = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public final ForSystemTimeSubClause getSubClause() {
    return subclause.symbolValue(ForSystemTimeSubClause.class);
  }
  public final SqlNode getTable() {
    return table;
  }

  public final SqlNode getStartDateTime() {
    return startDateTime;
  }

  public final SqlNode getEndDateTime() {
    return endDateTime;
  }

  public void setTable(SqlNode table) {
    this.table = table;
  }

  public void setStartDateTime(SqlNode startTime) {
    this.startDateTime = startTime;
  }

  public void setEndDateTime(SqlNode endTime) {
    this.endDateTime = endTime;
  }
  /**
   * <code>SqlForSystemTimeOperator</code> describes the syntax of the SQL <code>
   * SqlForSystemTime
   * </code> operator. Since there is only one such operator, this class is
   * almost certainly a singleton.
   */
  public static class SqlForSystemTimeOperator extends SqlOperator {
    //~ Constructors -----------------------------------------------------------
    private SqlForSystemTimeOperator() {
      super("ForSystemTime", SqlKind.FOR_SYSTEM_TIME, 20, true, null,
              null, null);
    }

    //method----------------------------------------------------------------
    public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    public SqlCall createCall(
            SqlLiteral functionQualifier,
            SqlParserPos pos,
            SqlNode... operands) {
      assert functionQualifier == null;
      return new SqlForSystemTime(pos,
                                  operands[0],
                                  (SqlLiteral) operands[1],
                                  operands[3],
                                  operands[4]);
    }

    @Override public void unparse(SqlWriter writer,
                                  SqlCall call,
                                  int leftPrec,
                                  int rightPrec) {
      SqlForSystemTime systemTimeClause = (SqlForSystemTime) call;

      final SqlWriter.Frame systemTimeFrame = writer.startList(SqlWriter.FrameTypeEnum.
              FOR_SYSTEM_TIME);

      switch (systemTimeClause.getSubClause()) {
      case AS_OF:
        systemTimeClause.table.unparse(writer, leftPrec, getLeftPrec());
        writer.sep("FOR SYSTEM TIME AS OF");
        systemTimeClause.startDateTime.unparse(writer, getRightPrec(), rightPrec);
        break;
      case FROM_TO:
        systemTimeClause.table.unparse(writer, leftPrec, getLeftPrec());
        writer.sep("FOR SYSTEM TIME FROM");
        systemTimeClause.startDateTime.unparse(writer, leftPrec, rightPrec);
        writer.sep("TO");
        systemTimeClause.endDateTime.unparse(writer, leftPrec, leftPrec);
        break;
      case BETWEEN_AND:
        systemTimeClause.table.unparse(writer, leftPrec, getLeftPrec());
        writer.sep("FOR SYSTEM TIME BETWEEN");
        systemTimeClause.startDateTime.unparse(writer, 0, 0);
        writer.sep("AND");
        systemTimeClause.endDateTime.unparse(writer, 0, getRightPrec());
        break;
      case CONTAINED_IN:
        systemTimeClause.table.unparse(writer, leftPrec, getLeftPrec());
        writer.sep("FOR SYSTEM TIME CONTAINED IN");
        final SqlWriter.Frame frame = writer.startList("(", ")");
        systemTimeClause.startDateTime.unparse(writer, 0, 0);
        writer.sep(",");
        systemTimeClause.endDateTime.unparse(writer, 0, 0);
        writer.endList(frame);
        break;
      default:
        Util.unexpected(systemTimeClause.getSubClause());
      }
      writer.endList(systemTimeFrame);
    }
  }
}
// End SqlForSystemTime.java
