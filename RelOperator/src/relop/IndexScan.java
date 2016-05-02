package relop;

import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
    private HeapFile file;
    private HashIndex indexScan;
    private BucketScan bucketScan;
    
    private boolean isOpen;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
      this.file = file;
      this.setSchema(schema);
      this.indexScan = index;
      this.bucketScan = index.openScan();
      
      this.isOpen = true;
              
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    indent(depth);
    System.out.println("Index Scan: "+indexScan.toString()+"\n");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
      if(isOpen() == true)
          this.bucketScan.close();
      
      this.bucketScan = indexScan.openScan();
      this.isOpen = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //return indexScan != null;
    return this.isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    if(isOpen() == false)
        throw new IllegalStateException("Random error : Scan already in closed state");
    this.bucketScan.close();
    this.isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
      if(!isOpen)
          throw new IllegalStateException("Random error : Scan not opened");
      
      return bucketScan.hasNext();
     
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(this.bucketScan.hasNext()) {
        Schema sch = this.getSchema();
        byte[] curID = this.file.selectRecord(this.bucketScan.getNext());
        
        if(curID == null) {
            throw new IllegalStateException("Random error : Last tuple reached");
        }
        
        return new Tuple(sch, curID);
    } else
        throw new IllegalStateException("Random error : No more tuples left to scan");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    if(isOpen)
        return bucketScan.getLastKey();
    else
        throw new IllegalStateException("Random error : Scan not open");
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
      if(isOpen)
          return bucketScan.getNextHash();
      else
          throw new IllegalStateException("Random error : Scan not open");
  }

    public HeapFile getFile() {
        return file;
    }

    public void setFile(HeapFile file) {
        this.file = file;
    }

    public HashIndex getIndexScan() {
        return indexScan;
    }

    public void setIndexScan(HashIndex indexScan) {
        this.indexScan = indexScan;
    }

    public BucketScan getBucketScan() {
        return bucketScan;
    }

    public void setBucketScan(BucketScan bucketScan) {
        this.bucketScan = bucketScan;
    }

    public boolean isIsOpen() {
        return isOpen;
    }

    public void setIsOpen(boolean isOpen) {
        this.isOpen = isOpen;
    }
  
  

} // public class IndexScan extends Iterator
