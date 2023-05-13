package cn.edu.thssdb.exception.InsertException;

public class StringEntryTooLongException extends Exception{
    @Override
    public String getMessage() {
        return "Entry String too long";
    }
}
