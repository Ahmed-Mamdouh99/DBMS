import java.io.*;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;

class MetaData {
  private static final String path = "data/meta.csv";
  private static MetaData meta;
  private Hashtable<String, HashSet<Column>> tableMeta;
  private Properties props;

  static boolean containsTable(String tableName) throws DBAppException {
    if (meta == null) {
      loadMetaData();
    }
    return meta.tableMeta.containsKey(tableName);
  }

  public static void validateQuery(SQLTerm[] terms, String[] operators) throws DBAppException {
    if (meta == null) {
      loadMetaData();
    }
    if (terms.length == 0 || operators.length != terms.length - 1) {
      throw new DBAppException("Could not validate query: Length of terms and operators invalid");
    }
    String tableName = terms[0]._strTableName;
    if (!meta.tableMeta.containsKey(tableName)) {
      throw new DBAppException("The table: " + tableName + " Does not exist");
    }
    HashSet<Column> columns = meta.tableMeta.get(tableName);
    for (SQLTerm term : terms) {
      if (!term._strTableName.equals(tableName)) {
        throw new DBAppException("Inconsistent table names in query");
      }
      Column column = null;
      for (Column c : columns) {
        if (c.getName().equals(term._strColumnName)) {
          column = c;
          break;
        }
      }
      if (column == null) {
        throw new DBAppException("Column: " + term._strColumnName + " Does not exist in the queried table");
      }
      try {
        Class.forName(column.getType()).cast(term._objValue);
      } catch (ClassNotFoundException e) {
        throw new DBAppException("The value is not consistent with the column data type");
      }
      switch (term._strOperator) {
        case ("="):
        case ("!="):
        case ("<"):
        case ("<="):
        case (">"):
        case (">="):
          break;
        default:
          throw new DBAppException("Invalid term operator");
      }
    }
    for (String operator : operators) {
      switch (operator) {
        case ("AND"):
        case ("OR"):
        case ("XOR"):
          break;
        default:
          throw new DBAppException("Invalid query operator");
      }
    }
  }

  private void writeToDisk() throws DBAppException {
    if (meta == null) {
      return;
    }
    try {
      PrintWriter pw = new PrintWriter(new FileOutputStream(path));
      for (HashSet<Column> table : meta.tableMeta.values()) {
        for (Column c : table) {
          pw.println(c);
        }
      }
      pw.flush();
      pw.close();
    } catch (FileNotFoundException e) {
      throw new DBAppException("Could not write metadata to disk: " + e.getMessage());
    }
  }

  static Table getTable(String tableName) throws DBAppException {
    if (meta == null) {
      loadMetaData();
    }
    String tableDirectory = "data/" + tableName + "/";
    return Table.loadTable(tableName, tableDirectory, meta.tableMeta.get(tableName));
  }

  static void createTable(String tableName, String key, Hashtable<String, String> colData) throws DBAppException {
    // Load metadata
    if (meta == null) {
      loadMetaData();
    }
    // Check if there is a table with the same name
    if (containsTable(tableName)) {
      throw new DBAppException("Table already exists with the same name: " + tableName);
    }
    // Check if the column data types are valid
    for (String dataType : colData.values()) {
      switch (dataType) {
        case ("java.lang.String"):
        case ("java.lang.Integer"):
        case ("java.lang.Double"):
        case ("java.lang.Boolean"):
        case ("java.util.Date"):
          break;
        default:
          throw new DBAppException("Invalid datatype for the column: " + dataType);
      }
    }
    // Check if the columns have a touch date column
    if (colData.containsKey("TouchDate")) {
      throw new DBAppException("Cannot create a column called TouchDate");
    }
    Table newTable = new Table(tableName, key, colData);
    // Add tableMeta
    meta.tableMeta.put(tableName, newTable.getColSet());
    meta.writeToDisk();
  }

  private static void loadMetaData() throws DBAppException {
    meta = new MetaData();
    meta.tableMeta = new Hashtable<>();
    File metaFile = new File(path);
    // Load properties
    try {
      meta.props = new Properties();
      meta.props.load(new FileInputStream("config/DBApp.properties"));
    } catch (IOException e) {
      throw new DBAppException("Could not read config: " + e.getMessage());
    }
    // Check if a csv exists to read from
    if (!metaFile.exists()) {
      return;
    }
    // Read data from csv
    try {
      BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
      while (bf.ready()) {
        String col = bf.readLine();
        Column loadedColumn = new Column(col);
        String tableName = loadedColumn.getTableName();
        if (!meta.tableMeta.containsKey(tableName)) {
          meta.tableMeta.put(tableName, new HashSet<>());
        }
        meta.tableMeta.get(tableName).add(loadedColumn);
      }
    } catch (IOException e) {
      throw new DBAppException("Could not read metadata: " + e.getMessage());
    }
  }

  static int getTablePageSize() throws DBAppException {
    if (meta == null) {
      loadMetaData();
    }
    return Integer.parseInt(meta.props.getProperty("MaximumRowsCountinPage"));
  }

  static int getIndexPageSize() throws DBAppException {
    if (meta == null) {
      loadMetaData();
    }
    return Integer.parseInt(meta.props.getProperty("BitmapSize"));
  }

  static boolean validateRecord(String tableName, Hashtable<String, Object> row) throws DBAppException {
    // Get table column data
    Hashtable<String, String> columns = getColumnData(tableName);
    // Check if the size of the row is the same as the table's
    if (row.size() != columns.size() - 1) {
      return false;
    }
    return validateMask(tableName, row);
  }

  static boolean validateMask(String tableName, Hashtable<String, Object> mask) throws DBAppException {
    Hashtable<String, String> columns = getColumnData(tableName);
    // Check that all values exist and validate the data types
    for (String key : mask.keySet()) {
      // Skip touch date
      if (key.equals("TouchDate")) {
        continue;
      }
      if (!columns.containsKey(key)) {
        return false;
      }
      try {
        Class.forName(columns.get(key)).cast(mask.get(key));
      } catch (ClassNotFoundException e) {
        return false;
      }
    }
    return true;
  }

  private static Hashtable<String, String> getColumnData(String tableName) throws DBAppException {
    if (meta == null) {
      loadMetaData();
    }
    Hashtable<String, String> out = new Hashtable<>();
    for (Column col : meta.tableMeta.get(tableName)) {
      out.put(col.getName(), col.getType());
    }
    return out;
  }
}
