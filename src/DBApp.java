import java.io.File;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {

  public DBApp() throws DBAppException {
    init();
  }

  public void init() throws DBAppException {
    // Make sure that the data directory exists
    File dataFile = new File("data/");
    if(!dataFile.exists()) {
      if(!dataFile.mkdir()){
        throw new DBAppException("Could not create data directory");
      }
    }
  }

  public void createTable(String tableName, String keyColumn, Hashtable<String, String> columns) throws DBAppException {
    MetaData.createTable(tableName, keyColumn, columns);
  }

  public void insertIntoTable(String tableName, Hashtable<String, Object> record) throws DBAppException {
    // Get the table instance from the metaData
    Table table = getTableFromMeta(tableName, record);
    // Adding the record to the table
    table.insert(record);
  }

  public void updateTable(String tableName, String keyCol, Hashtable<String, Object> record) throws DBAppException {
    // Get table from meta
    Table table = getTableFromMeta(tableName, record);
    table.update(keyCol, record);
  }

  public void deleteFromTable(String tableName, Hashtable<String, Object> mask) throws DBAppException {
    // Get table from meta
    Table table = MetaData.getTable(tableName);
    // Create a comparable hashtable
    table.delete(mask);
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
    MetaData.validateQuery(terms, operators);
    return table.select(terms, operators);
  }

  private Table getTableFromMeta(String tableName, Hashtable<String, Object> record) throws DBAppException {
    // Check if the table exists
    if (!MetaData.containsTable(tableName)) {
      throw new DBAppException("The table " + tableName + " does not exist.");
    }
    // Validate record
    if(!MetaData.validateMask(tableName, record)){
      throw new DBAppException("Row data invalid.");
    }
    return MetaData.getTable(tableName);
  }
}
