package cn.edu.thssdb.impl;

import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.condition.SingleConditionPlan;
import cn.edu.thssdb.plan.impl.SelectPlan;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SelectImpl {

  private static Boolean isWhereConditionPrimary(Table table, MultipleConditionPlan cond) {
    if (!cond.hasChild()) {
      SingleConditionPlan singleConditionPlan = cond.singleConditionPlan;
      String columnName2 = singleConditionPlan.expr1.columnName;
      return table.Column2Index(columnName2) == table.primaryIndex;
    }
    return false;
  }

  private static Boolean isPrimarySelect(SelectPlan plan, Database db, Long sessionId) {
    MultipleConditionPlan cond = plan.getWhereConditionPlan();
    if (cond != null && !cond.hasChild()) {
      SingleConditionPlan singleConditionPlan = cond.singleConditionPlan;

      if (!Objects.equals(singleConditionPlan.comparator, "=")) return false;

      String tableName = plan.getTableNameList().get(0);
      String columnName = singleConditionPlan.expr1.columnName;
      Table table = db.getTableWithoutLock(tableName);
      //          Entry key = new Entry((Comparable) singleConditionPlan.expr2.getValue());
      //          ArrayList<Row> res = table.getRowsByPrimaryKey(key);
      //          System.out.println("[Primary select] " + res.size());
      //          targetTable = new QueryTable(table, res);
      return table.Column2Index(columnName) == table.primaryIndex;
    }
    return false;
  }

  private static Boolean allPrimaryKeyJoin(SelectPlan plan, Database db, Long sessionId) {
    List<String> targetTableList = plan.getTableNameList();
    MultipleConditionPlan onConditionPlan = plan.getOnConditionPlan();

    if (targetTableList.size() == 2 && !onConditionPlan.hasChild) {
      SingleConditionPlan singleConditionPlan = onConditionPlan.singleConditionPlan;
      if (singleConditionPlan.expr1.type == ComparerType.COLUMN
          && singleConditionPlan.expr2.type == ComparerType.COLUMN) {
        String tableName1 = singleConditionPlan.expr1.tableName;
        String tableName2 = singleConditionPlan.expr2.tableName;
        String columnName1 = singleConditionPlan.expr1.columnName;
        String columnName2 = singleConditionPlan.expr2.columnName;

        Table table1 = db.getTableWithoutLock(tableName1);
        Table table2 = db.getTableWithoutLock(tableName2);

        int primary1 = table1.primaryIndex;
        int primary2 = table2.primaryIndex;
        //            System.out.println("[ALL PRIMARY KEY JOIN]" + primary1 + " " + primary2);
        //            System.out.println(
        //                columnName1
        //                    + " "
        //                    + columnName2
        //                    + " "
        //                    + tableHandler1.getTable().getColumns().get(primary1).getName()
        //                    + " "
        //                    + tableHandler2.getTable().getColumns().get(primary2).getName());
        //        System.out.printf("%s %s %s %s %n", columnName1, columnName2,
        // table1.getColumns().get(primary1).getName(),
        // table1.getColumns().get(primary1).getName());
        return columnName1.equals(table1.getColumns().get(primary1).getName())
            && columnName2.equals(table2.getColumns().get(primary2).getName());
      }
    }
    return false;
  }

  public static QueryTable handleSelectPlan(SelectPlan plan, Database db, Long sessionId) {

    // TODO: Parallelism, use multi-thread to verify the conditions.

    // build the target query table

    List<String> targetTableList = plan.getTableNameList();
    QueryTable targetTable = null;
    MultipleConditionPlan onConditionPlan = plan.getOnConditionPlan();
    ArrayList<String> columnNames = new ArrayList<>();

    // TODO: check "="

    // special case: no join, select primary key
    if (targetTableList.size() == 1 && isPrimarySelect(plan, db, sessionId)) {
      MultipleConditionPlan cond = plan.getWhereConditionPlan();
      if (!cond.hasChild()) {
        SingleConditionPlan singleConditionPlan = cond.singleConditionPlan;
        String tableName = plan.getTableNameList().get(0);
        String columnName = singleConditionPlan.expr1.columnName;
        try (Table.TableHandler tableHandler =
            db.getTableForSession(sessionId, tableName, true, false)) {
          Table table = tableHandler.getTable();
          if (table.Column2Index(columnName) == table.primaryIndex) {
            Entry key = new Entry((Comparable) singleConditionPlan.expr2.getValue());
            ArrayList<Row> res = table.getRowsByPrimaryKey(key);
            //            System.out.println("[Primary select] " + res.size());
            targetTable = new QueryTable(table, res);

            for (Column column : targetTable.columns) {
              columnNames.add(column.getName());
            }
          }
        }
      }
    } else if (allPrimaryKeyJoin(plan, db, sessionId)) {

      SingleConditionPlan singleConditionPlan = onConditionPlan.singleConditionPlan;
      String tableName1 = singleConditionPlan.expr1.tableName;
      String tableName2 = singleConditionPlan.expr2.tableName;
      ArrayList<Row> res = new ArrayList<>();
      try (Table.TableHandler tableHandler1 =
          db.getTableForSession(sessionId, tableName1, true, false)) {
        try (Table.TableHandler tableHandler2 =
            db.getTableForSession(sessionId, tableName2, true, false)) {
          Table table1 = tableHandler1.getTable();
          Table table2 = tableHandler2.getTable();

          // if where condition is also primary key
          if (isWhereConditionPrimary(table2, plan.getWhereConditionPlan())) {
            SingleConditionPlan singleConditionPlan1 =
                plan.getWhereConditionPlan().singleConditionPlan;
            Entry key = new Entry((Comparable) singleConditionPlan1.expr2.getValue());
            for (Row row : table1.getRowsByPrimaryKey(key)) {
              res.addAll(table2.getRowsByPrimaryKey(key));
            }
          } else {
            for (Row row : table1) {
              Entry attr1 = row.getEntries().get(table1.primaryIndex);
              //            System.out.println(attr1);
              ArrayList<Row> findRows = table2.getRowsByPrimaryKey(attr1);
              res.addAll(findRows);
            }
          }
          //          System.out.println("[PRIMARY JOIN] " + res.size());
          targetTable = new QueryTable(table1, table2, res);

          for (Column column : targetTable.columns) {
            columnNames.add(column.getName());
          }
        }
      }
    } else {

      try (Table.TableHandler tableHandler =
          db.getTableForSession(sessionId, targetTableList.get(0), true, false)) {
        targetTable = new QueryTable(tableHandler.getTable(), targetTableList.size() > 1);
      }
      for (int i = 1; i < targetTableList.size(); i++) {
        try (Table.TableHandler tableHandler =
            db.getTableForSession(sessionId, targetTableList.get(i), true, false)) {
          targetTable.joinWithTable(tableHandler.getTable());
        }
      }

      //      System.out.println(targetTable.toString());

      for (Column column : targetTable.columns) {
        columnNames.add(column.getName());
      }

      // on condition in table query
      if (onConditionPlan != null) {
        Iterator<Row> rowIterator = targetTable.rows.iterator();

        List<Row> rowToDelete = new ArrayList<>();
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          if (!onConditionPlan.ConditionVerify(row, columnNames)) {
            rowToDelete.add(row);
          }
        }

        targetTable.rows.removeAll(rowToDelete);
      }
    }

    //    System.out.println(targetTable.toString());

    // where condition
    MultipleConditionPlan whereConditionPlan = plan.getWhereConditionPlan();
    if (whereConditionPlan != null) {
      Iterator<Row> rowIterator = targetTable.rows.iterator();

      List<Row> rowToDelete = new ArrayList<>();
      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        if (!whereConditionPlan.ConditionVerify(row, columnNames)) {
          rowToDelete.add(row);
        }
      }
      targetTable.rows.removeAll(rowToDelete);
    }

    // select the columns of result table
    List<String> attributeList = plan.getAttributeList();
    ArrayList<Column> selectedColumns = new ArrayList<>();
    ArrayList<Row> selectedRows = new ArrayList<>();
    if (attributeList.contains("*")) {
      selectedColumns.addAll(targetTable.columns);
      selectedRows.addAll(targetTable.rows);
    } else {
      // get the selected index of columns
      ArrayList<Integer> selectedColumnsIndex = new ArrayList<>();
      for (int i = 0; i < attributeList.size(); i++) {
        for (int j = 0; j < columnNames.size(); j++) {
          if (attributeList.get(i).equals(columnNames.get(j))) {
            selectedColumnsIndex.add(j);
            selectedColumns.add(targetTable.columns.get(j));
            break;
          }
        }
      }

      targetTable.columns = selectedColumns;

      // select the columns in rows
      for (Row row : targetTable.rows) {
        selectedRows.add(row.selectEntry(selectedColumnsIndex));
      }
    }

    targetTable.rows = selectedRows;
    return targetTable;
  }
}
