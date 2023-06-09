package cn.edu.thssdb.impl;

import cn.edu.thssdb.exception.AttributeValueNotMatchException;
import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.InsertException.AttributeNameNotExistException;
import cn.edu.thssdb.exception.InsertException.StringEntryTooLongException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.plan.impl.InsertPlan;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.List;

public class InsertImpl {
  public static void handleInsertPlan(InsertPlan plan, Database currentDB, Long sessionId)
      throws TableNotExistException, AttributeValueNotMatchException,
          AttributeNameNotExistException, StringEntryTooLongException {
    if (!currentDB.isTableExist(plan.getTableName())) {
      throw new TableNotExistException(plan.getTableName());
    }

    try (Table.TableHandler tableHandler =
        currentDB.getTableForSession(sessionId, plan.getTableName(), false, true)) {

      Table table = tableHandler.getTable();

      // transform the String list of entry value the entry list

      List<String> attrNameList = plan.getAttributeNameList();
      List<List<String>> entryList = plan.getEntryValueList();

      for (List<String> entryStringList : entryList) {

        if (attrNameList.size() != entryStringList.size() && attrNameList.size() != 0) {
          throw new AttributeValueNotMatchException();
        }

        ArrayList<Entry> entries = new ArrayList<>();
        for (int i = 0; i < entryStringList.size(); i++) {
          String entryString = entryStringList.get(i);
          ColumnType type = ColumnType.INT;

          int maxLength = 0;
          if (attrNameList.size() != 0) {

            boolean matched = false;
            for (Column column : table.getColumns()) {
              if (column.getName().equals(attrNameList.get(i))) {
                type = column.getType();
                maxLength = column.getMaxLength();
                matched = true;
                break;
              }
            }

            if (!matched) {
              throw new AttributeNameNotExistException(attrNameList.get(i));
            }
          } else {
            type = table.getColumns().get(i).getType();
            if (type == ColumnType.STRING) {
              maxLength = table.getColumns().get(i).getMaxLength();
            }
          }
          Entry entry = new Entry(1);

          switch (type) {
            case INT:
              entry = new Entry(Integer.parseInt(entryString));
              break;
            case LONG:
              entry = new Entry(Long.parseLong(entryString));
              break;
            case FLOAT:
              entry = new Entry(Float.parseFloat(entryString));
              break;
            case DOUBLE:
              entry = new Entry(Double.parseDouble(entryString));
              break;
            case STRING:
              entryString = entryString.substring(1, entryString.length() - 1);
              if (entryString.length() > maxLength) {
                System.out.printf(
                    "Too long %d %d %s%n", entryString.length(), maxLength, entryString);
                throw new StringEntryTooLongException();
              }
              entry = new Entry(entryString);
              break;
            default:
              System.out.println("type error" + type);
          }

          entries.add(entry);
        }

        /*
         * Undo Log format
         * INSERT <TABLE NAME> <ROW data>
         * */
        if (Global.ENABLE_ROLLBACK) {
          currentDB.undoLogger.writeLog(
              String.format("INSERT %s %s", plan.getTableName(), (new Row(entries))));
        }

        ArrayList<String> tmp = new ArrayList<>(attrNameList);
        table.insert(entries, tmp);
      }
      //      System.out.println(table.printTable());
    } catch (DuplicateKeyException e) {
      System.out.println("[Duplicate Key]");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
