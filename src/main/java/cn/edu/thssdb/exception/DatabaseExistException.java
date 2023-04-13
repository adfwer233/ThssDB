package cn.edu.thssdb.exception;

public class DatabaseExistException extends RuntimeException{

    private String databaseName;
    public DatabaseExistException(String databaseName) {
        this.databaseName = databaseName;
    }
    @Override
    public String getMessage() {
        return String.join("Exception: Database", this.databaseName, " already exists");
    }
}
