package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class ShowDatabasePlan extends LogicalPlan {
    private String databaseName;

    public ShowDatabasePlan(String databaseName) {
        super(LogicalPlan.LogicalPlanType.SHOW_DB);
        this.databaseName = databaseName;
    }
}

