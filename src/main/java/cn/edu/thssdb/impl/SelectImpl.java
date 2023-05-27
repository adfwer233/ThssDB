package cn.edu.thssdb.impl;

import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.impl.SelectPlan;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SelectImpl {
  public static QueryTable handleSelectPlan(SelectPlan plan, Database db, Long sessionId) {

    // TODO: Parallelism, use multi-thread to verify the conditions.

    // build the target query table

    List<String> targetTableList = plan.getTableNameList();
    QueryTable targetTable;

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

    ArrayList<String> columnNames = new ArrayList<>();
    for (Column column : targetTable.columns) {
      columnNames.add(column.getName());
    }

    // on condition in table query
    MultipleConditionPlan onConditionPlan = plan.getOnConditionPlan();
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
