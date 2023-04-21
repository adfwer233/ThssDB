package cn.edu.thssdb.exception;

public class TableNotExistException extends Exception{
    @Override
    public String getMessage() {
        return "No such table";
    }
}
