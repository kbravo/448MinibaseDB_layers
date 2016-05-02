package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
    private HashIndex indexScan;
    private SearchKey search_key;
    private HeapFile file;
    
    private RID curID;
    private HashScan hashScan;
    
    private boolean isOpen;
    
    /**
     * Constructs an index scan, given the hash index and schema.
     */
    public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
        this.file = file;
        this.setSchema(schema);
        this.indexScan = index;
        this.search_key = key;
        this.curID = new RID();
        this.hashScan = index.openScan(this.search_key);
        
        this.isOpen = true;
    }

    /**
     * Gives a one-line explaination of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
      indent(depth);
      System.out.println("Key Scan: " + hashScan.toString() +"\n");
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        if(isOpen == false) {
            this.hashScan = this.indexScan.openScan(this.search_key);
            this.isOpen = true;
            
        }
        else {
            this.hashScan.close();
            this.hashScan = indexScan.openScan(search_key);
            this.isOpen = true;
        }
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
      return isOpen;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
      hashScan.close();
      isOpen = false;
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        if(isOpen() == false)
            throw new IllegalStateException("Random error : Scan not opened");
        
        if(hashScan.hasNext() == true)
         return true;
        else
         return false;
    }

    /**
     * Gets the next tuple in the iteration.
     * 
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        if(isOpen() == false)
            throw new IllegalStateException("Random error : Scan not opened");
        
        curID = this.hashScan.getNext();
        Tuple ret = new Tuple(this.getSchema(), this.file.selectRecord(curID));
        return ret;
    }

  
  /**
    * Getters and Setter for Hash Joins
    */
  
    public HashIndex getIndexScan() {
        return indexScan;
    }

    public void setIndexScan(HashIndex indexScan) {
        this.indexScan = indexScan;
    }

    public SearchKey getSearch_key() {
        return search_key;
    }

    public void setSearch_key(SearchKey search_key) {
        this.search_key = search_key;
    }

    public HeapFile getFile() {
        return file;
    }

    public void setFile(HeapFile file) {
        this.file = file;
    }

    public HashScan getHashScan() {
        return hashScan;
    }

    public void setHashScan(HashScan hashScan) {
        this.hashScan = hashScan;
    }

    public boolean isIsOpen() {
        return isOpen;
    }

    public void setIsOpen(boolean isOpen) {
        this.isOpen = isOpen;
    }
} // public class KeyScan extends Iterator
