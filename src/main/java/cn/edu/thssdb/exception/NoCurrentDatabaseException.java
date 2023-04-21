package cn.edu.thssdb.exception;

public class NoCurrentDatabaseException extends Exception {
  @Override
  public String getMessage() {
    return "No Database Using";
  }
}
