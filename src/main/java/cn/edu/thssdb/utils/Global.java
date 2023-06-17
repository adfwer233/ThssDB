package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 24;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;
  public static String DEFAULT_USER_NAME = "root";
  public static String DEFAULT_PASSWORD = "root";

  public static String CLI_PREFIX = "ThssDB2023>";
  public static final String SHOW_TIME = "show time;";
  public static final String CONNECT = "connect";
  public static final String DISCONNECT = "disconnect;";
  public static final String QUIT = "quit;";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  public static final String DBMS_PATH = "thssDB";

  public static final String META_SUFFIX = "_meta";

  public static final String INDEX_SUFFIX = "_index";

  public static final String PAGE_SUFFIX = "_page_";

  public enum IsolationLevel {
    READ_COMMITTED,
    SERIALIZABLE
  }

  public static final IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

  public static Boolean ENABLE_ROLLBACK = false;
}
