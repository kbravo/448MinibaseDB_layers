package query;

import global.Minibase;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_CreateIndex;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

    String fileName;
    
    String table;
    
    String column_name;
    Schema schema;
    

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
    
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
    fileName = tree.getFileName(); //Getting the name of the file
    QueryCheck.fileNotExists(fileName);
    
    table = tree.getIxTable(); //Getting the table
    QueryCheck.tableExists(table);
    
    schema = Minibase.SystemCatalog.getSchema(table); //Get the schema for the file
    
    column_name = tree.getIxColumn();// Getting the column to setup the hash index on
    QueryCheck.columnExists(schema, column_name);
    
    IndexDesc[] arr = Minibase.SystemCatalog.getIndexes(fileName); //Get the Indexes in the database
    for(int i = 0; i < arr.length; i++) { //Check if the column already has a hash index on the column already
        if(arr[i].columnName.compareTo(column_name) == 0) {
            throw new QueryException("Hash index on this column exists already");
        }
    }
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HashIndex index = new HashIndex(fileName);
    HeapFile file = new HeapFile(table);
    
    FileScan fscan = new FileScan(schema, file);
    int field_number = schema.fieldNumber(column_name);
    
    while(fscan.hasNext()) {
        Tuple tuple = fscan.getNext();
        SearchKey key = new SearchKey(tuple.getField(field_number));
        index.insertEntry(key, fscan.getLastRID());
    }
    fscan.close();
    
    Minibase.SystemCatalog.createIndex(fileName, table, column_name);
    System.out.println("Index " + column_name + " built on table " + table);
    
  } // public void execute()

} // class CreateIndex implements Plan
