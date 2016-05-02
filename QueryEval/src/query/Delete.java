package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import parser.AST_Delete;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Selection;
import relop.Tuple;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {
        String tableName;
        Schema schema;
        Predicate predicates[][];
        
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
      
      tableName = tree.getFileName();
      // QueryCheck.tableExists(tableName);
     //QueryCheck.
      //schema = Minibase.SystemCatalog.getSchema(tableName);
     schema = QueryCheck.tableExists(tableName);
      predicates = tree.getPredicates();
     QueryCheck.predicates(schema, predicates);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
      int count = 0;
      
      HeapFile h = new HeapFile(tableName);
      FileScan hscan = new FileScan(schema,h);

      //HeapScan hscan = new HeapScan();
      //HeapScan hscan = h.openScan();
      IndexDesc indes[] = Minibase.SystemCatalog.getIndexes(tableName);
     /* 
      while(hscan.hasNext())
      {
          RID rid = new RID();
          byte[] barr = hscan.getNext(rid);
          Tuple tuple = new Tuple(schema,barr);
          
          if(pcheck(tuple))
          {
             count++;
             
             for(IndexDesc ind:indes)
             {
                 HashIndex hi = new HashIndex(ind.indexName);
                 SearchKey key = new SearchKey(tuple.getField(ind.columnName));
                 hi.deleteEntry(key, rid);
             }
             h.deleteRecord(rid);
          }
        
      }
    */
     
     Selection sel = new Selection(hscan,predicates[0]);    


     for(int i = 1; i<predicates.length; i++)
     {
         for(int j = 0; j<predicates[i].length; j++)
         {
             sel = new Selection(sel, predicates[i][j]);
         }
     }
     
     while(sel.hasNext())
     {
         h.deleteRecord(hscan.getLastRID());
         count++;
     }
     
//Minibase.SystemCatalog.d;
    // print the output message

    delete(count);
    System.out.println(count+" are the number of rows affected");
    hscan.close();
  } // public void execute()

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Predicate[][] getPredicates() {
        return predicates;
    }

    public void setPredicates(Predicate[][] predicates) {
        this.predicates = predicates;
    }

  public boolean pcheck(Tuple tup)
  {
      int i = 0;
      int j = 0;
      boolean check = false;
      for(i=0;i<predicates.length;i++)
      {
          for(j=0;j<predicates.length;j++)
          {
              if(predicates[i][j].evaluate(tup))
              {
                  check = true;
                  break;
              }
          }
      }
      return check;
  }
  
  public void delete(int count)
  {
      RID rid = Minibase.SystemCatalog.getFileRID(tableName,true);
      byte[] arr = Minibase.SystemCatalog.f_rel.selectRecord(rid);
      Tuple tuple = new Tuple(Minibase.SystemCatalog.s_rel,arr);
      int rCount =tuple.getIntFld(1);
      tuple.setIntFld(1, rCount-count);
      Minibase.SystemCatalog.f_rel.updateRecord(rid, tuple.getData());
  }
  
} // class Delete implements Plan
