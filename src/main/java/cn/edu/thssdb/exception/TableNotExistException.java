package cn.edu.thssdb.exception;

public class TableNotExistException extends Exception {
  private String tableName;

  public TableNotExistException() {
    super();
    tableName = "";
  }

  public TableNotExistException(String tableName) {
    super();
    this.tableName = tableName;
  }

  @Override
  public String getMessage() {
    return "No table ".concat(tableName);
  }
}
