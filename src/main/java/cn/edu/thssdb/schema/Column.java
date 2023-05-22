package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private boolean primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, boolean primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String getName() {
    return name;
  }

  public ColumnType getType() {
    return type;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public boolean isPrimary() {
    return primary;
  }

  public void setPrimary() {
    primary = true;
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }

  public static Column createColumnFromMeta(String columnMeta) {
    String[] parseRes = columnMeta.split(",");
    Column res =
        new Column(
            parseRes[0],
            ColumnType.valueOf(parseRes[1]),
            Boolean.parseBoolean(parseRes[2]),
            Boolean.parseBoolean(parseRes[3]),
            Integer.parseInt(parseRes[4]));
    return res;
  }
}
