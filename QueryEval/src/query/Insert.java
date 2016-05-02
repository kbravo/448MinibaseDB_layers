package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

    String fileName;
    Object[] values;
    Schema schema;
    String ixTable;
    HeapFile file;
    Tuple tuple;
    RID rid;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
      fileName = tree.getFileName();
      QueryCheck.tableExists(fileName);
      
      values = tree.getValues();
      schema = Minibase.SystemCatalog.getSchema(fileName);
      
      QueryCheck.insertValues(schema, values);
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    file = new HeapFile(fileName);
    tuple = new Tuple(schema, values);
    rid = file.insertRecord(tuple.getData());
   
    // update indexes of this table
    IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(fileName);
    for (IndexDesc in : indexes) {
        //Getting the column name and index name
        String indexName = in.indexName;
        String colName = in.columnName;
        
        //inserting the new hash index entry
        HashIndex index = new HashIndex(indexName);
        SearchKey key = new SearchKey(tuple.getField(colName));
        index.insertEntry(key, rid);
    }
    
    //Updating the System Catalog
    RID rid = Minibase.SystemCatalog.getFileRID(fileName, true);
    byte[] data = Minibase.SystemCatalog.f_rel.selectRecord(rid);
    Tuple tuple = new Tuple(Minibase.SystemCatalog.s_rel, data);
    int recCount = tuple.getIntFld(1);
    tuple.setIntFld(1, recCount + 1);
    Minibase.SystemCatalog.f_rel.updateRecord(rid, tuple.getData());

    // print the output message
    System.out.println("MiniSQL: 1 row affected.");
  } // public void execute()

} // class Insert implements Plan
