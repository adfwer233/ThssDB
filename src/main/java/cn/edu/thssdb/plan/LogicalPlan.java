package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_DB,
    DROP_DB,
    USE_DB,
    CREATE_TABLE,
    SHOW_TABLE,
    SHOW_DB,
    DROP_TABLE,
    INSERT,
    SELECT,
    DELETE,
    QUIT,
    COMPARER,
    CONDITION,
    MULTIPLE_CONDITION
  }
}
