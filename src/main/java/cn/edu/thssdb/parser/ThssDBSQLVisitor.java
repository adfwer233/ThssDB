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

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;

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

  // TODO: parser to more logical plan
}
