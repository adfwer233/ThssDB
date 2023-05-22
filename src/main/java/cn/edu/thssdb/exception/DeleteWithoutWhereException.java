package cn.edu.thssdb.exception;

public class DeleteWithoutWhereException extends Exception {
  @Override
  public String getMessage() {
    return "Exception: Delete without where";
  }
}
