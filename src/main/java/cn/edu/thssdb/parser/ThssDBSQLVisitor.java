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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.TypeNotMatchException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.condition.ComparerPlan;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.condition.SingleConditionPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    return new DropDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
    return new UseDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    // get tablename
    String tableName = ctx.tableName().getText();

    // parse column definitions and generate column list
    List<SQLParser.ColumnDefContext> columnDefs = ctx.columnDef();
    ArrayList<Column> columns = new ArrayList<Column>();
    System.out.println(columnDefs.size());
    System.out.println(columnDefs.get(0).getText());

    List<String> primaryKeys = new ArrayList<String>();

    if (ctx.tableConstraint() != null) {
      primaryKeys =
          ctx.tableConstraint().columnName().stream()
              .map(tmpCtx -> tmpCtx.getText().toUpperCase())
              .collect(Collectors.toList());
    }

    for (SQLParser.ColumnDefContext columnDef : columnDefs) {

      String columnName = columnDef.columnName().getText();
      String columnTypeString = columnDef.typeName().getChild(0).getText().toUpperCase();
      System.out.println(columnTypeString);

      ColumnType type = ColumnType.valueOf(columnTypeString);

      int maxLength = 0;
      if (type == ColumnType.STRING) {
        maxLength = Integer.parseInt(columnDef.typeName().NUMERIC_LITERAL().toString());
      }

      List<String> columnConstraints =
          columnDef.columnConstraint().stream()
              .map(tmpCtx -> tmpCtx.getText().toUpperCase())
              .collect(Collectors.toList());

      System.out.println(columnConstraints);

      Column column =
          new Column(
              columnName,
              type,
              primaryKeys.contains(columnName.toUpperCase()),
              columnConstraints.contains("NOTNULL"),
              maxLength);
      columns.add(column);
    }

    CreateTablePlan plan = new CreateTablePlan(tableName, columns);

    return plan;
  }

  @Override
  public LogicalPlan visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    return new ShowTablePlan(ctx.tableName().getText());
  }

  public LogicalPlan visitShowDbStmt(SQLParser.ShowDbStmtContext ctx) {
    return new ShowDatabasePlan(ctx.getText());
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new DropTablePlan(ctx.tableName().getText());
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    List<SQLParser.ColumnNameContext> columnNameCtxList = ctx.columnName();
    List<SQLParser.ValueEntryContext> valueEntryContextList = ctx.valueEntry();

    List<String> columnNameList =
        columnNameCtxList.stream().map(x -> x.getText()).collect(Collectors.toList());

    List<List<String>> valueEntryList = new ArrayList();
    for (SQLParser.ValueEntryContext valueEntryContext : valueEntryContextList) {
      valueEntryList.add(
          valueEntryContext.literalValue().stream()
              .map(x -> x.getText())
              .collect(Collectors.toList()));
    }

    InsertPlan plan = new InsertPlan(ctx.tableName().getText(), columnNameList, valueEntryList);
    return plan;
  }

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    // get tableName
    String tableName = ctx.tableName().getText();

    if (ctx.K_WHERE() == null) {
      System.out.println("Exception: Delete without where");
    }
    // 递归获得Condition
    MultipleConditionPlan whereCond = null;
    if (ctx.multipleCondition() != null) {
      whereCond = visitMultipleCondition(ctx.multipleCondition());
    }
    return new DeletePlan(tableName, whereCond);
  }

  @Override
  public ComparerPlan visitComparer(SQLParser.ComparerContext ctx) {
    if (ctx.columnFullName() != null) {
      String tableName = null;
      if (ctx.columnFullName().tableName() != null) {
        tableName = ctx.columnFullName().tableName().IDENTIFIER().getText();
      }
      String columnName = ctx.columnFullName().columnName().IDENTIFIER().getText();
      return new ComparerPlan(ComparerType.COLUMN, tableName, columnName);
    } else if (ctx.literalValue() != null) {
      String literalValue = "null";
      if (ctx.literalValue().NUMERIC_LITERAL() != null) {
        literalValue = ctx.literalValue().NUMERIC_LITERAL().getText();
        return new ComparerPlan(ComparerType.NUMBER, literalValue);
      } else if (ctx.literalValue().STRING_LITERAL() != null) {
        literalValue = ctx.literalValue().STRING_LITERAL().getText();
        return new ComparerPlan(ComparerType.STRING, literalValue);
      }
      return new ComparerPlan(ComparerType.NULL, literalValue);
    }
    return null;
  }

  @Override
  public ComparerPlan visitExpression(SQLParser.ExpressionContext ctx) {
    if (ctx.comparer() != null) {
      return (ComparerPlan) visit(ctx.comparer());
    } else if (ctx.expression().size() == 1) {
      return (ComparerPlan) visit(ctx.getChild(1));
    } else {
      ComparerPlan comparerPlan1 = (ComparerPlan) visit(ctx.getChild(0));
      ComparerPlan comparerPlan2 = (ComparerPlan) visit(ctx.getChild(2));

      if ((comparerPlan1.type != ComparerType.NUMBER && comparerPlan1.type != ComparerType.COLUMN)
          || (comparerPlan2.type != ComparerType.NUMBER
              && comparerPlan2.type != ComparerType.COLUMN)) {
        throw new TypeNotMatchException(comparerPlan1.type, ComparerType.NUMBER);
      }

      ComparerPlan newComparerPlan =
          new ComparerPlan(comparerPlan1, comparerPlan2, ctx.getChild(1).getText());
      newComparerPlan.type = ComparerType.NUMBER;

      return newComparerPlan;
    }
  }

  @Override
  public SingleConditionPlan visitCondition(SQLParser.ConditionContext ctx) {
    ComparerPlan comparerPlan1 = (ComparerPlan) visit(ctx.getChild(0));
    ComparerPlan comparerPlan2 = (ComparerPlan) visit(ctx.getChild(2));
    String op = ctx.getChild(1).getText();

    return new SingleConditionPlan(comparerPlan1, comparerPlan2, op);
  }

  @Override
  public MultipleConditionPlan visitMultipleCondition(SQLParser.MultipleConditionContext ctx) {
    if (ctx.getChildCount() == 1) {
      return new MultipleConditionPlan((SingleConditionPlan) visit(ctx.getChild(0)));
    }

    MultipleConditionPlan m1 = (MultipleConditionPlan) visit(ctx.getChild(0));
    MultipleConditionPlan m2 = (MultipleConditionPlan) visit(ctx.getChild(2));
    String op = ctx.getChild(1).getText();

    return new MultipleConditionPlan(m1, m2, op);
  }

  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    List<String> attributeList =
        ctx.resultColumn().stream().map(x -> x.getText()).collect(Collectors.toList());
    List<String> targetTableList =
        ctx.tableQuery(0).tableName().stream().map(x -> x.getText()).collect(Collectors.toList());
    MultipleConditionPlan whereConditionPlan = null;
    MultipleConditionPlan onConditionPlan = null;
    if (ctx.tableQuery(0).multipleCondition() != null) {
      onConditionPlan = visitMultipleCondition(ctx.tableQuery(0).multipleCondition());
    }
    if (ctx.multipleCondition() != null) {
      whereConditionPlan = visitMultipleCondition(ctx.multipleCondition());
    }

    return new SelectPlan(attributeList, targetTableList, onConditionPlan, whereConditionPlan);
  }

  @Override
  public LogicalPlan visitQuitStmt(SQLParser.QuitStmtContext ctx) {
    return new QuitPlan();
  }

  // TODO: parser to more logical plan
}
