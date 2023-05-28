package cn.edu.thssdb.service;

import cn.edu.thssdb.impl.PlanHandler;
import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
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
import java.util.Arrays;
import java.util.Date;
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

    System.out.println("[Statement] " + req.statement);

    // TODO: maintain a map from session id to current database
    long currentSessionId = req.getSessionId();

    Manager manager = Manager.getInstance();

    // begin transaction
    if (req.statement.toLowerCase().equals("begin transaction")) {
      if (manager.currentSessions.contains(currentSessionId)) {
        return new ExecuteStatementResp(
            StatusUtil.fail("This session already in a Transaction"), false);
      } else {
        manager.currentSessions.add(currentSessionId);
        return new ExecuteStatementResp(StatusUtil.success("Transaction begin"), false);
      }
    }

    // commit
    if (req.statement.toLowerCase().equals("commit;")) {
      if (!manager.currentSessions.contains(currentSessionId)) {
        return new ExecuteStatementResp(
            StatusUtil.fail("This session not in a Transaction now"), false);
      } else {
        // TODO: handle lock here
        manager.currentSessions.remove(currentSessionId);
        manager.releaseTransactionLocks(currentSessionId);
        manager.persistCurrentDatabase(currentSessionId);
        return new ExecuteStatementResp(StatusUtil.success("Transaction end"), false);
      }
    }

    if (req.statement.toLowerCase().equals("rollback;")) {
      if (!manager.currentSessions.contains(currentSessionId)) {
        return new ExecuteStatementResp(
            StatusUtil.fail("This session not in a Transaction now"), false);
      } else {
        manager.rollbackCurrentTransaction(currentSessionId);
        return new ExecuteStatementResp(StatusUtil.success("Rollback success"), false);
      }
    }

    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement);

    ArrayList<LogicalPlan.LogicalPlanType> logType =
        new ArrayList<>(
            Arrays.asList(
                LogicalPlan.LogicalPlanType.CREATE_TABLE,
                LogicalPlan.LogicalPlanType.DROP_TABLE,
                LogicalPlan.LogicalPlanType.INSERT,
                LogicalPlan.LogicalPlanType.DELETE));

    /*/
    only log in transaction environment, autocommit needs no log
    */
    if (logType.contains(plan.getType()) && manager.currentSessions.contains(currentSessionId)) {
      try (Database.DatabaseHandler db =
          manager.getCurrentDatabase(currentSessionId, true, false)) {
        db.getDatabase().logger.writeLog(req.statement);
      } catch (Exception e) {
        return new ExecuteStatementResp(StatusUtil.fail("Write Log failed"), false);
      }
    }

    if (manager == null) System.out.println("manager is null");

    ExecuteStatementResp resp = PlanHandler.handlePlan(plan, currentSessionId, manager);

    if (logType.contains(plan.getType()) && !manager.currentSessions.contains(currentSessionId)) {
      manager.persistCurrentDatabase(currentSessionId);
      manager.releaseTransactionLocks(currentSessionId);
    }

    return resp;
  }
}
