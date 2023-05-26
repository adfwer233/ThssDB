package cn.edu.thssdb.impl;

import cn.edu.thssdb.exception.DeleteWithoutWhereException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.NoCurrentDatabaseException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;

import java.util.ArrayList;
import java.util.List;

public class PlanHandler {
  public static ExecuteStatementResp handlePlan(
      LogicalPlan plan, Long currentSessionId, Manager manager) {

    switch (plan.getType()) {
      case CREATE_DB:
        System.out.println("[DEBUG] " + plan);
        CreateDatabasePlan createPlan = (CreateDatabasePlan) plan;
        try {
          manager.createDatabaseIfNotExists(createPlan.getDatabaseName());
          manager.persist();
          System.out.println("create success");
          return new ExecuteStatementResp(StatusUtil.success("Create Success"), false);
        } catch (Exception e) {
          System.out.print(e);
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DROP_DB:
        System.out.println("Drop database success");

        DropDatabasePlan dropPlan = (DropDatabasePlan) plan;
        try {
          manager.deleteDatabase(dropPlan.getDatabaseName());
          manager.persist();
          return new ExecuteStatementResp(StatusUtil.success("Drop success"), false);
        } catch (KeyNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case USE_DB:
        System.out.println("Use database success");

        UseDatabasePlan usePlan = (UseDatabasePlan) plan;
        try {
          manager.switchDatabase(currentSessionId, usePlan.getDatabaseName());
          return new ExecuteStatementResp(StatusUtil.success("Use success"), false);
        } catch (KeyNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case CREATE_TABLE:
        CreateTablePlan createTablePlan = (CreateTablePlan) plan;
        try (Database.DatabaseHandler currentDatabaseHandler =
            manager.getCurrentDatabase(currentSessionId, false, true)) {
          Database currentDatabase = currentDatabaseHandler.getDatabase();
          if (currentDatabase == null) throw new NoCurrentDatabaseException();
          List<Column> columnList = createTablePlan.getColumns();
          System.out.println(columnList.size());

          Column[] columnsArray = columnList.stream().toArray(Column[]::new);
          String tmpString = createTablePlan.getTableName();
          currentDatabase.create(createTablePlan.getTableName(), columnsArray);
          currentDatabase.persist();
          return new ExecuteStatementResp(
              StatusUtil.success(currentDatabase.getTableInfo(createTablePlan.getTableName())),
              false);
        } catch (KeyNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (NoCurrentDatabaseException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (TableNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SHOW_TABLE:
        ShowTablePlan showTablePlan = (ShowTablePlan) plan;
        try (Database.DatabaseHandler currentDatabaseHandler =
            manager.getCurrentDatabase(currentSessionId, true, false)) {
          Database currentDatabase = currentDatabaseHandler.getDatabase();
          if (currentDatabase == null) throw new NoCurrentDatabaseException();
          String res = currentDatabase.getTableInfo(showTablePlan.getTableName());
          return new ExecuteStatementResp(StatusUtil.success(res), false);
        } catch (NoCurrentDatabaseException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (TableNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail("Exception: " + e.getMessage()), false);
        }
      case DROP_TABLE:
        DropTablePlan dropTablePlan = (DropTablePlan) plan;
        try (Database.DatabaseHandler currentDatabaseHandler =
            manager.getCurrentDatabase(currentSessionId, false, true)) {
          Database currentDataBase = currentDatabaseHandler.getDatabase();
          if (currentDataBase == null) throw new NoCurrentDatabaseException();
          currentDataBase.drop(dropTablePlan.getTableName());
          return new ExecuteStatementResp(StatusUtil.success(dropTablePlan.getTableName()), false);
        } catch (NoCurrentDatabaseException | TableNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SHOW_DB:
        System.out.println("Show databases success");
        String res = Manager.getInstance().showDb();
        return new ExecuteStatementResp(StatusUtil.success(res), false);
      case INSERT:
        InsertPlan insertPlan = (InsertPlan) plan;
        System.out.println("insert begin " + currentSessionId + manager.currentSessions);
        try (Database.DatabaseHandler currentDatabaseHandler =
            manager.getCurrentDatabase(currentSessionId, false, true)) {
          System.out.println("get handler success");
          InsertImpl.handleInsertPlan(
              insertPlan, currentDatabaseHandler.getDatabase(), currentSessionId);
          return new ExecuteStatementResp(StatusUtil.success("Insert success"), false);
        } catch (Exception e) {
          System.out.println("Insert failed" + e.getMessage());
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DELETE:
        System.out.println("DELETE");
        DeletePlan deletePlan = (DeletePlan) plan;
        try (Database.DatabaseHandler currentDatabaseHandler =
            manager.getCurrentDatabase(currentSessionId, false, true)) {
          Database currentDataBase = currentDatabaseHandler.getDatabase();
          if (currentDataBase == null) throw new NoCurrentDatabaseException();
          try (Table.TableHandler tableHandler =
              currentDataBase.getTableForSession(
                  currentSessionId, deletePlan.getTableName(), false, true)) {
            Table currentTable = tableHandler.getTable();
            ArrayList<String> columnNames = new ArrayList<>();
            ArrayList<Column> columns = currentTable.getColumns();
            for (Column c : columns) {
              columnNames.add(c.getName());
            }
            MultipleConditionPlan whereCond = ((DeletePlan) plan).getWhereCond();
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
                    currentDataBase.undoLogger.writeLog(String.format("DELETE %s %s", deletePlan.getTableName(), row.toString()));
                  }

                }
              }
            }
            return new ExecuteStatementResp(StatusUtil.success(currentTable.tableName), false);
          }
        } catch (NoCurrentDatabaseException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (DeleteWithoutWhereException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SELECT:
        SelectPlan selectPlan = (SelectPlan) plan;
        try (Database.DatabaseHandler currentDatabaseHandler =
            manager.getCurrentDatabase(currentSessionId, true, false)) {
          QueryTable queryTable =
              SelectImpl.handleSelectPlan(
                  selectPlan, currentDatabaseHandler.getDatabase(), currentSessionId);
          return new ExecuteStatementResp(StatusUtil.success(queryTable.toString()), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case QUIT:
        try (Database.DatabaseHandler currentDatabaseHandler =
            manager.getCurrentDatabase(currentSessionId, false, true)) {
          Database database = currentDatabaseHandler.getDatabase();
          database.quit();
          return new ExecuteStatementResp(StatusUtil.success("quit success"), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      default:
        System.out.println("Not Implemented");
    }
    return null;
  }
}
