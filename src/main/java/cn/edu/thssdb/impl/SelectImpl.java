package cn.edu.thssdb.impl;

import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.impl.SelectPlan;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SelectImpl {
  public static QueryTable handleSelectPlan(SelectPlan plan, Database db) {

    // TODO: Parallelism, use multi-thread to verify the conditions.

    // build the target query table

    List<String> targetTableList = plan.getTableNameList();
    QueryTable targetTable = new QueryTable(db.getTable(targetTableList.get(0)));
    for (int i = 1; i < targetTableList.size(); i++) {
      targetTable.joinWithTable(db.getTable(targetTableList.get(i)));
    }

    ArrayList<String> columnNames = new ArrayList<>();
    for (Column column : targetTable.columns) {
      columnNames.add(column.getName());
    }

    System.out.println(targetTable.toString());

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
      System.out.println(rowToDelete);
      targetTable.rows.removeAll(rowToDelete);
    }

    // select the columns of result table
    List<String> attributeList = plan.getAttributeList();
    System.out.println(attributeList);
    ArrayList<Column> selectedColumns = new ArrayList<>();
    ArrayList<Row> selectedRows = new ArrayList<>();
    if (attributeList.contains("*")) {
      selectedColumns.addAll(targetTable.columns);
      selectedRows.addAll(targetTable.rows);
    } else {
      // TODO: select the columns
    }

    targetTable.rows = selectedRows;

    return targetTable;
  }
}
