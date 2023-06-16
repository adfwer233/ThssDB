package cn.edu.thssdb.impl;

import cn.edu.thssdb.exception.DeleteWithoutWhereException;
import cn.edu.thssdb.exception.NoCurrentDatabaseException;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.condition.SingleConditionPlan;
import cn.edu.thssdb.plan.impl.DeletePlan;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;

public class DeleteImpl {
  public static void handleDeletePlan(
      DeletePlan deletePlan, Database currentDataBase, Long currentSessionId)
      throws NoCurrentDatabaseException, DeleteWithoutWhereException {
    if (currentDataBase == null) throw new NoCurrentDatabaseException();
    try (Table.TableHandler tableHandler =
        currentDataBase.getTableForSession(
            currentSessionId, deletePlan.getTableName(), false, true)) {
      Table currentTable = tableHandler.getTable();

      if (!deletePlan.getWhereCond().hasChild) {
        SingleConditionPlan singleConditionPlan = deletePlan.getWhereCond().singleConditionPlan;
        if (singleConditionPlan.expr1.type == ComparerType.COLUMN
            && singleConditionPlan.expr2.type != ComparerType.COLUMN && singleConditionPlan.comparator.equals("=")) {
          String attrName = singleConditionPlan.expr1.columnName;
          if (currentTable.Column2Index(attrName) == currentTable.primaryIndex) {
            currentTable.removePrimaryKey(
                new Entry((Comparable) singleConditionPlan.expr2.getValue()));
            return;
          }
        }
      }

      ArrayList<String> columnNames = new ArrayList<>();
      ArrayList<Column> columns = currentTable.getColumns();
      for (Column c : columns) {
        columnNames.add(c.getName());
      }
      MultipleConditionPlan whereCond = deletePlan.getWhereCond();
      if (whereCond == null) {
        throw new DeleteWithoutWhereException();
      } else {
        for (Row row : currentTable) {
          if (whereCond.ConditionVerify(row, columnNames)) {
            currentDataBase.DeleteRow(row, currentTable.tableName);

            /*
             * Undo Format
             * DELETE <TABLE_NAME> <ROW CONTENT>
             * */
            if (Global.ENABLE_ROLLBACK) {
              currentDataBase.undoLogger.writeLog(
                  String.format("DELETE %s %s", deletePlan.getTableName(), row.toString()));
            }
          }
        }
      }
    }
  }
}
