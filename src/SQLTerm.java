class SQLTerm {
  String _strTableName;
  String _strColumnName;
  String _strOperator;
  Object _objValue;

  private boolean validate() throws DBAppException {
    // Check if the table exists
    if (!MetaData.containsTable(_strTableName)) {
      return false;
    }
    Table table = MetaData.getTable(_strTableName);
    // Check if the column exists
    if (!table.containsColumn(_strColumnName)) {
      return false;
    }
    Column column = table.getColumn(_strColumnName);
    // Check if the value fits the column type
    String type = column.getType();
    try {
      Class.forName(type).cast(_objValue);
    } catch (ClassNotFoundException ignored) {
      return false;
    }
    // Validate the operator
    return !type.equals("java.lang.Boolean") || (_strOperator.equals("=") || _strOperator.equals("!="));
  }

  static boolean validateQuery(SQLTerm[] terms, String[] operators) throws DBAppException {
    if(terms.length != operators.length-1){
      return false;
    }
    for (String operator : operators) {
      switch (operator) {
        case ("OR"):
        case ("AND"):
        case ("XOR"):
          continue;
        default:
          return false;
      }
    }
    for(SQLTerm term : terms) {
      if(!term.validate()){
        return false;
      }
    }
    return true;
  }
}
