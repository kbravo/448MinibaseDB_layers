package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;


/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
    private HeapFile file;
    private HeapScan hfscan;
    private boolean isOpen;
    
    private RID curId;

    /**
    * Constructs a file scan, given the schema and heap file.
    */
    public FileScan(Schema schema, HeapFile file) {
      
        setSchema(schema); // setting the schema with the inherited function
        
        this.hfscan = file.openScan();
        this.file = file;

        isOpen = true; // Just opened file scans
        this.curId = new RID(); // new RID()
    }

    /**
    * Gives a one-line explaination of the iterator, repeats the call on any
    * child iterators, and increases the indent depth along the way.
    */
    public void explain(int depth) {
      indent(depth);
      System.out.println("File Scan: "+file.toString()+"\n");
    }

    /**
    * Restarts the iterator, i.e. as if it were just constructed.
    */
    public void restart() {
      if(isOpen)
          close();
      hfscan = file.openScan();
      isOpen = true;
      //pin counts
    }

    /**
    * Returns true if the iterator is open; false otherwise.
    */
    public boolean isOpen() {
        return this.isOpen;
    }

    /**
    * Closes the iterator, releasing any resources (i.e. pinned pages).
    */
    public void close() {
        if(isOpen() == true) {
            hfscan.close();
            isOpen = false;
        } else {
            throw new IllegalStateException("Random error : Scan already in closed state");
        }
    }

    /**
    * Returns true if there are more tuples, false otherwise.
    */
    public boolean hasNext() {
      if(isOpen == false)
          throw new IllegalStateException("Random error : Scan not opened");
      
      if(hfscan.hasNext() == true)
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
        if(hasNext() == false) {
           throw new IllegalStateException("Random error : No more tuples left to scan");
        }
        Schema schema = getSchema();
        return new Tuple(schema, this.hfscan.getNext(curId));
    }

    /**
    * Gets the RID of the last tuple returned.
    */
    public RID getLastRID() {
        return new RID(curId);
    }
    
    /**
     * Getters and Setter for Hash Joins
     */

    public HeapFile getFile() {
        return file;
    }

    public void setFile(HeapFile file) {
        this.file = file;
    }

    public HeapScan getHfscan() {
        return hfscan;
    }

    public void setHfscan(HeapScan hfscan) {
        this.hfscan = hfscan;
    }

    public boolean isIsOpen() {
        return isOpen;
    }

    public void setIsOpen(boolean isOpen) {
        this.isOpen = isOpen;
    }

    public RID getCurId() {
        return curId;
    }

    public void setCurId(RID curId) {
        this.curId = curId;
    }

} // public class FileScan extends Iterator
