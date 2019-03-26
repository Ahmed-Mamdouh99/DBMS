import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {

  public void init() {

  }

  public void createTable(String tableName, String keyColumn, Hashtable<String, String> columns) throws DBAppException {
    MetaData.createTable(tableName, keyColumn, columns);
  }

  public void insertIntoTable(String tableName, Hashtable<String, Object> record) throws DBAppException {
    // Check if the table exists
    if (!MetaData.containsTable(tableName)) {
      throw new DBAppException("The table " + tableName + " does not exist.");
    }
    // Validate record
    if(!MetaData.validateRecord(tableName, record)){
      throw new DBAppException("Row data invalid.");
    }
    // Get the table instance from the metaData
    Table table = MetaData.getTable(tableName);
    // Adding the record to the table
    table.insert(record);
  }

  public void updateTable(String tableName, String keyCol, Hashtable<String, Object> record) throws DBAppException {
    // Check if the table exists
    if (!MetaData.containsTable(tableName)) {
      throw new DBAppException("The table " + tableName + " does not exist.");
    }
    //noinspection Duplicates
    // Validate record
    if(!MetaData.validateMask(tableName, record)){
      throw new DBAppException("Row data invalid.");
    }
    // Get table from meta
    Table table = MetaData.getTable(tableName);
    table.update(keyCol, record);
    //System.out.printf("Updated %d records.\n", table.update(keyCol, record));
  }

  public void deleteFromTable(String tableName, Hashtable<String, Object> mask) throws DBAppException {
    // Check if the table exists
    if (!MetaData.containsTable(tableName)) {
      throw new DBAppException("The table " + tableName + " does not exist.");
    }
    // Validate record
    if(!MetaData.validateMask(tableName, mask)){
      throw new DBAppException("Row data invalid.");
    }
    // Get table from meta
    Table table = MetaData.getTable(tableName);
    // Create a comparable hashtable
    table.delete(mask);
    //System.out.printf("Deleted %d records.\n", table.delete(mask));
  }

  public void createBitmapIndex(String tableName, String colName) throws DBAppException {
    Table table = MetaData.getTable(tableName);
    table.createBitmapIndex(colName);
  }

  public Iterator selectFromTable(SQLTerm[] terms, String[] operators) throws DBAppException {
    String tableName = terms[0]._strTableName;
    // Check if the table exists
    if (!MetaData.containsTable(tableName)) {
      throw new DBAppException("The table " + tableName + " does not exist.");
    }
    Table table = MetaData.getTable(tableName);
    return table.select(terms, operators);
  }
}
