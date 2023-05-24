package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class QuitPlan extends LogicalPlan {
  public QuitPlan() {
    super(LogicalPlanType.QUIT);
  }
}
