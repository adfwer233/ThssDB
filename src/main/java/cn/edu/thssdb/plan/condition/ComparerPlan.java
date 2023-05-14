package cn.edu.thssdb.plan.condition;

import cn.edu.thssdb.exception.TypeNotMatchException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.utils.Cell;

import java.lang.reflect.Array;
import java.security.KeyException;
import java.util.ArrayList;

public class ComparerPlan extends LogicalPlan {
    public String tableName = null;
    public String columnName;
    public String literalValue;
    public ComparerType type;

    public ComparerPlan comparerLeft;
    public ComparerPlan comparerRight;

    public String op;
    public boolean isNull;
    public boolean hasChild;

    public ComparerPlan() {
        super(LogicalPlanType.COMPARER);
        this.type = ComparerType.NULL;
        this.literalValue = "null";
        this.isNull = true;
        this.hasChild = false;
    }

    public ComparerPlan(ComparerType type, String tableName, String columnName) {
        super(LogicalPlanType.COMPARER);
        if (type == ComparerType.COLUMN) {
            this.type = type;
            this.tableName = tableName;
            this.columnName = columnName;
            this.hasChild = false;
        } else {
            throw new TypeNotMatchException(type, ComparerType.COLUMN);
        }
    }

    public ComparerPlan(ComparerType type, String literalValue) {
        super(LogicalPlanType.COMPARER);
        this.type = type;
        this.literalValue = literalValue;
        this.hasChild = false;
        if (type == ComparerType.NULL) {
            this.isNull = true;
        }
    }
    public ComparerPlan(ComparerPlan comparerLeft, ComparerPlan comparerRight, String op) {
        super(LogicalPlanType.COMPARER);
        this.comparerLeft = comparerLeft;
        this.comparerRight = comparerRight;
        this.op = op;
        this.hasChild =true;
    }

    public Object getValue(Row row, ArrayList<String> ColumnName) {
        try {
            if (type == ComparerType.COLUMN) {
                int index = -1;
                if(this.tableName != null) {
                    String columnFullName = this.tableName + "_" + this.columnName;
                    index = ColumnName.indexOf(columnFullName);
                }
                if (index == -1) {
                    index = ColumnName.indexOf(this.columnName);
                }
                Entry entry = row.getEntries().get(index);
                if (entry == null) {
                    throw new KeyException();
                }
                return entry.value;
            } else if (type == ComparerType.NUMBER) {
                if (literalValue.contains(".")) {
                    return Double.parseDouble(literalValue);
                } else {
                    return Integer.parseInt(literalValue);
                }
            } else if (type == ComparerType.STRING) {
                return literalValue;
            }
            return null;
        } catch (KeyException e) {
            System.out.println("Get Error in ComparerItem.getValue(Row,ArrayList<String>): " + e.getMessage());
            return null;
        }
    }

    public Object getValue() {
        if (type == ComparerType.COLUMN) {
            throw new TypeNotMatchException(ComparerType.COLUMN, ComparerType.NUMBER);
        } else if (type == ComparerType.NUMBER) {
            if (literalValue.contains(".")) {
               return Double.parseDouble(literalValue);
            } else {
                return Integer.parseInt(literalValue);
            }
        } else if (type == ComparerType.STRING) {
            return literalValue;
        }
        return null;
    }

    public Double ComparerRes(Row row, ArrayList<String> ColumnName) {
        if (!hasChild) {
            Object value1 = getValue(row, ColumnName);
            if (value1 == null || value1 instanceof String) {
                return null;
            }
            return Double.parseDouble(value1.toString());
        } else {
            Double valueLeft = this.comparerLeft.ComparerRes(row, ColumnName);
            Double valueRight = this.comparerRight.ComparerRes(row, ColumnName);
            Double value;
            switch (op) {
                case "+":
                    value = valueLeft + valueRight;
                    break;
                case "-":
                    value = valueLeft - valueRight;
                    break;
                case "*":
                    value = valueLeft * valueRight;
                    break;
                case "/":
                    value = valueLeft / valueRight;
                    break;
                default:
                    value = 0.0;
                    break;
            }
            String newLiteralValue;
            if (value.intValue() == value.doubleValue()) {
                newLiteralValue = String.valueOf(value.intValue());
            } else {
                newLiteralValue = value.toString();
            }
            this.literalValue = newLiteralValue;
            return value;
        }
    }
    public boolean isNull() { return this.isNull;}
}
