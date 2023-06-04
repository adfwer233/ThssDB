package cn.edu.thssdb.impl;

import cn.edu.thssdb.exception.NoCurrentDatabaseException;
import cn.edu.thssdb.plan.condition.ComparerPlan;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.condition.SingleConditionPlan;
import cn.edu.thssdb.plan.impl.UpdatePlan;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;
import java.util.Iterator;

public class UpdateImpl {

  public static void handleUpdatePlan(
      UpdatePlan updatePlan, Database currentDB, Long currentSessionId)
      throws NoCurrentDatabaseException {

    Database currentDataBase = currentDB;

    if (currentDataBase == null) throw new NoCurrentDatabaseException();
    try (Table.TableHandler tableHandler =
        currentDataBase.getTableForSession(
            currentSessionId, updatePlan.getTableName(), false, true)) {

      Table currentTable = tableHandler.getTable();

      // handle the special case: where attr is primary key
      if (!updatePlan.getWhereCond().hasChild) {
        SingleConditionPlan singleConditionPlan = updatePlan.getWhereCond().singleConditionPlan;
        if (singleConditionPlan.expr1.type == ComparerType.COLUMN
            && singleConditionPlan.expr2.type != ComparerType.COLUMN) {
          String attrName = singleConditionPlan.expr1.columnName;
          if (currentTable.Column2Index(attrName) == currentTable.primaryIndex) {
            currentTable.updateByPrimaryKey(
                new Entry((Comparable) singleConditionPlan.expr2.getValue()),
                updatePlan.getColumnName(),
                new Entry((Comparable) updatePlan.getExpr().getValue()));
            return;
          }
        }
      }

      // 获取columnNames
      ArrayList<String> columnNames = new ArrayList<>();
      ArrayList<Column> columns = currentTable.getColumns();
      for (Column c : columns) {
        columnNames.add(c.getName());
      }

      MultipleConditionPlan whereCond = updatePlan.getWhereCond();

      ArrayList<Row> row2Update = new ArrayList<>();
      Iterator<Row> rowIterator = currentTable.iterator();
      if (whereCond == null) {
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          row2Update.add(row);
        }
      } else {
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          if (whereCond.ConditionVerify(row, columnNames)) {
            row2Update.add(row);
          }
        }
      }

      // Update
      String columnName = updatePlan.getColumnName();
      int index = currentTable.Column2Index(columnName);
      ComparerPlan expr = updatePlan.getExpr();
      Entry newEntry = new Entry((Comparable) expr.getValue());

      for (Row row : row2Update) {
        Row newRow = new Row();
        ArrayList<Entry> entries = row.getEntries();
        for (int i = 0; i < entries.size(); i++) {
          if (i == index) {
            newRow.getEntries().add(newEntry);
          } else {
            newRow.getEntries().add(entries.get(i));
          }
        }
        Entry primaryE = entries.get(currentTable.getPrimaryIndex());
        currentTable.update(primaryE, newRow, columnNames);
      }
      System.out.println("UPDATE");
    }
  }
}
