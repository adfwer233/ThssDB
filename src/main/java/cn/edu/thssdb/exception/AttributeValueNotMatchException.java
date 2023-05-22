package cn.edu.thssdb.exception;

public class AttributeValueNotMatchException extends Exception {

  @Override
  public String getMessage() {
    return "The number of attribute and entry value is not match in Insert clause";
  }
}
