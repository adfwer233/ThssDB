package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.DeleteWithoutWhereException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.NoCurrentDatabaseException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.impl.InsertImpl;
import cn.edu.thssdb.impl.SelectImpl;
import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: maintain a map from session id to current database
    long currentSessionId = req.getSessionId();

    Manager manager = Manager.getInstance();

    if (req.statement.equals("commit;")) {
      try {
        manager.getCurrentDatabase(currentSessionId).persist();
        return new ExecuteStatementResp(StatusUtil.success("commit success"), false);
      } catch (Exception e) {
        return new ExecuteStatementResp(StatusUtil.fail("commit fail"), false);
      }
    }

    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement);

    if (manager == null) System.out.println("manager is null");

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
        try {
          Database currentDatabase = manager.getCurrentDatabase(currentSessionId);

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
        }
      case SHOW_TABLE:
        ShowTablePlan showTablePlan = (ShowTablePlan) plan;
        try {
          Database currentDatabase = manager.getCurrentDatabase(currentSessionId);
          if (currentDatabase == null) throw new NoCurrentDatabaseException();
          String res = currentDatabase.getTableInfo(showTablePlan.getTableName());
          return new ExecuteStatementResp(StatusUtil.success(res), false);
        } catch (NoCurrentDatabaseException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (TableNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DROP_TABLE:
        DropTablePlan dropTablePlan = (DropTablePlan) plan;
        try {
          Database currentDataBase = manager.getCurrentDatabase(currentSessionId);
          if (currentDataBase == null) throw new NoCurrentDatabaseException();
          currentDataBase.drop(dropTablePlan.getTableName());
          return new ExecuteStatementResp(StatusUtil.success(dropTablePlan.getTableName()), false);
        } catch (NoCurrentDatabaseException | TableNotExistException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SHOW_DB:
        System.out.println("Show databases success");
        String res = Manager.getInstance().showDb();
        return new ExecuteStatementResp(StatusUtil.success(res), false);
      case INSERT:
        InsertPlan insertPlan = (InsertPlan) plan;
        try {
          InsertImpl.handleInsertPlan(insertPlan, manager.getCurrentDatabase(currentSessionId));
          return new ExecuteStatementResp(StatusUtil.success("Insert success"), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DELETE:
        System.out.println("DELETE");
        DeletePlan deletePlan = (DeletePlan) plan;
        try {
          Database currentDataBase = manager.getCurrentDatabase(currentSessionId);
          if (currentDataBase == null) throw new NoCurrentDatabaseException();
          Table currentTable = currentDataBase.getTables().get(deletePlan.getTableName());
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
                return new ExecuteStatementResp(StatusUtil.success(currentTable.tableName), false);
              }
            }
          }
        } catch (NoCurrentDatabaseException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } catch (DeleteWithoutWhereException e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SELECT:
        SelectPlan selectPlan = (SelectPlan) plan;
        try {
          QueryTable queryTable =
              SelectImpl.handleSelectPlan(selectPlan, manager.getCurrentDatabase(currentSessionId));
          return new ExecuteStatementResp(StatusUtil.success(queryTable.toString()), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      default:
        System.out.println("Not Implemented");
    }
    return null;
  }
}
