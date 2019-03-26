import java.io.File;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Objects;

public class DBAppTest {

  private static final String[] types = {"java.lang.String", "java.lang.Integer", "java.lang.Double",
      "java.lang.Boolean", "java.util.Date"};
  private static final PrintWriter pw = new PrintWriter(System.out);

  public static void main(String[] args) {
    // Clear data directory
    File dataFile = new File("data/");
    if (!deleteDirectories(dataFile)) {
      System.err.println("Could not clear data directory");
      return;
    }
    // Run tests
    test();
    // flush writer
    pw.flush();
    pw.close();
  }

  private static void test() {
    DBApp app = new DBApp();
    // Test table creation
    int tableNumber = testTableCreation(app, 1);
    if (tableNumber == -1) {
      // end testing
      return;
    }
    String tableName = "Table" + tableNumber;
    // Test insertion without indices
    testInsertion(app, tableName, tableNumber, 2);
    // Test index creation
    testIndexCreation(app, tableName, tableNumber);
    // Test insertion with indices
    testInsertion(app, tableName, tableNumber, 6);
    // Test selection
    testSelection(app, tableName, tableNumber);
    // Test deletion
    testDeletion(app, tableName, tableNumber);
    // Test selection to make sure all true records were deleted
    testSelection(app, tableName, tableNumber);
    // Test update
    testUpdate(app, tableName, tableNumber);
    // Checking if all records did change
    selectAll(app, tableName);
  }

  private static void testUpdate(DBApp app, String tableName, int tableNumber) {
    Hashtable<String, Object> mask = new Hashtable<>();
    mask.put(types[3] + tableNumber, false);
    mask.put(types[1] + tableNumber, -1);
    try {
      app.updateTable(tableName, types[3] + tableNumber, mask);
    } catch (DBAppException e) {
      e.printStackTrace();
      return;
    }
    pw.println("Updated records successfully.");
  }

  private static int testTableCreation(DBApp app, int numberOfTables) {
    Hashtable<String, String> columns = new Hashtable<>();
    int i = 0;
    for (; i < numberOfTables; i++) {
      String tableName = "Table" + i;
      columns.clear();
      for (String type : types) {
        columns.put(type + i, type);
      }
      String keyCol = types[0] + i;
      try {
        app.createTable(tableName, keyCol, columns);
      } catch (DBAppException ignored) {
        break;
      }
    }
    pw.printf("Created %d/%d tables.\n", i, numberOfTables);
    return i - 1;
  }

  private static void testInsertion(DBApp app, String tableName, int tableNumber, int numberOfRecords) {
    Hashtable<String, Object> record = new Hashtable<>();
    int i = 0;
    for (; i < numberOfRecords; i++) {
      // Clearing the hash table
      record.clear();
      // Generating random data
      Comparable<?>[] values = genRecord();
      for (int j = 0; j < types.length; j++) {
        record.put(types[j] + tableNumber, values[j]);
      }
      try {
        app.insertIntoTable(tableName, record);
      } catch (DBAppException e) {
        e.printStackTrace();
        break;
      }
    }
    pw.printf("Inserted %d/%d rows.\n", i, numberOfRecords);
  }

  private static void testIndexCreation(DBApp app, String tableName, int tableNumber) {
    int i = 0;
    for (; i < types.length; i++) {
      String type = types[i];
      String colName = type + tableNumber;
      try {
        app.createBitmapIndex(tableName, colName);
      } catch (DBAppException e) {
        e.printStackTrace();
        break;
      }
    }
    pw.printf("Created %d/%d indices.\n", i, types.length);
  }

  private static void testSelection(DBApp app, String tableName, int tableNumber) {
    SQLTerm term = new SQLTerm();
    term._objValue = true;
    term._strOperator = "=";
    term._strColumnName = types[3] + tableNumber;
    term._strTableName = tableName;
    Iterator<Hashtable<String, Object>> result;
    try {
      result = app.selectFromTable(new SQLTerm[]{term}, new String[0]);
    } catch (DBAppException e) {
      pw.println("Selection failed.");
      return;
    }
    int resultSize = printResults(result);
    pw.printf("Selection succeeded with %d matches.\n", resultSize);
  }

  private static void testDeletion(DBApp app, String tableName, int tableNumber) {
    // Deleting all true values from the table
    Hashtable<String, Object> record = new Hashtable<>();
    record.put(types[3] + tableNumber, true);
    try {
      app.deleteFromTable(tableName, record);
    } catch (DBAppException e) {
      e.printStackTrace();
    }
    pw.println("Deleted records successfully.");
  }


  // private helpers
  private static void selectAll(DBApp app, String tableName) {
    try {
      SQLTerm term = new SQLTerm();
      term._strTableName = tableName;
      Iterator<Hashtable<String, Object>> result = app.selectFromTable(new SQLTerm[]{term}, new String[0]);
      printResults(result);
    } catch (DBAppException e) {
      e.printStackTrace();
    }
  }

  private static Comparable<?>[] genRecord() {
    String string = genWord(4);
    int integer = (int) (Math.random() * 100);
    double floatingPoint = Math.random();
    boolean bool = Math.random() > 0.5;
    Date date = new Date();
    return new Comparable<?>[]{string, integer, floatingPoint, bool, date};
  }

  private static String genWord(int length) {
    int lowerBound = 65;
    int upperBound = 91;
    StringBuilder output = new StringBuilder();
    for (int i = 0; i < length; i++) {
      char c = (char) ((int) ((Math.random() * (upperBound - lowerBound) + lowerBound)));
      output.append(c);
    }
    return output.toString();
  }

  private static boolean deleteDirectories(File root) {
    for (File subFile : Objects.requireNonNull(root.listFiles())) {
      if (subFile.isFile()) {
        if (!subFile.delete()) {
          return false;
        }
        continue;
      }
      if (!deleteDirectories(subFile)) {
        return false;
      }
      if (!subFile.delete()) {
        return false;
      }
    }
    return true;
  }

  private static int printResults(Iterator<Hashtable<String, Object>> result) {
    pw.println("Result set:");
    int resultSize = 0;
    while (result.hasNext()) {
      resultSize++;
      Hashtable<String, Object> record = result.next();
      StringBuilder recString = new StringBuilder();
      for (Object value : record.values()) {
        recString.append(" ").append(value);
      }
      pw.println(recString);
    }
    return resultSize;
  }
}
