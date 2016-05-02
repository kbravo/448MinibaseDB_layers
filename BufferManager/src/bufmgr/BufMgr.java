//include the disk manager
package bufmgr;
/*
 * Team member 1 - Kartik Yadav (yadav11)
 * Team member 2 - Jaideep Juneja (juneja0)
 * 
 */
import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import chainexception.ChainException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;


public class BufMgr implements GlobalConst {
	
	//Descriptor object
	static public class Descriptor {
		PageId pagenumber;
		int usage;
		int pin_count;
		boolean dirtybit;
		public Descriptor() {
			this.pagenumber = new PageId();
			this.pin_count = 0;
			this.dirtybit = false;
			this.usage = 0;
		}
		
	}
	
	Descriptor[] bufDescr;
	Page[] bufpool;
	
	int bufsize; // to maintain the current pages pinned
	
	String replaceArg; // strategy for replacement
	Hashtable table; //hashtable for <PageId, frameNumber>
	/*
	 * Constructor
	 */
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {

		this.bufDescr = new Descriptor[numbufs];
		this.bufpool = new Page[numbufs];
		
		for(int i = 0; i < numbufs; i++) {
			this.bufDescr[i] = new Descriptor();
			this.bufpool[i] = new Page();
		}
		
		this.table = new Hashtable();
		this.replaceArg = "LFU";
		bufsize = 0;
	}
	/*
	 * Makes a new page from disk manager
	 */
	public PageId newPage(Page firstpage, int howmany) throws ChainException, IOException {
		PageId id = Minibase.DiskManager.allocate_page(howmany);
		
		try {
			pinPage(id, firstpage, true);
		} catch(ChainException e) {
			for (int i = 0; i < howmany; i++) {
				id.pid += i;
				Minibase.DiskManager.deallocate_page(id);
	        }
			return null;
		}
		return id;
	}
	/*
	 * Pins a page
	 */
	public void pinPage(PageId pageno, Page page, boolean emptyPage) throws InvalidPageNumberException, FileIOException, IOException, ChainException { 
		if(this.table.contains(pageno))
		{
			int index = this.table.get(pageno);
			if(emptyPage) {
				throw new IllegalArgumentException(
				          "Page pinned; PIN_MEMCPY not allowed");
			}
			this.bufDescr[index].usage++;
			this.bufDescr[index].pin_count++;
			page.setPage(this.bufpool[index]);
		} else {
			int capacity = this.bufpool.length;
			if(this.bufsize == capacity) {
				
				ArrayList<Integer> list = getUnpinnedIndexes();
				
				if(list.size() == 0)
				{
					throw new BufferPoolExceededException(null, "Buffer Pool has been exceeded");
				}
				
				int minUsage = this.bufDescr[list.get(0).intValue()].usage;
				int minindex = list.get(0).intValue();
				
				for(Integer i : list)
				{
					if(this.bufDescr[i.intValue()].usage < minUsage)
					{
						minUsage = this.bufDescr[i.intValue()].usage;
						minindex = i.intValue();
					}
				}
				
				if(this.bufDescr[minindex].dirtybit == true) {
					Minibase.DiskManager.write_page(this.bufDescr[minindex].pagenumber, this.bufpool[minindex]);
					this.bufDescr[minindex].dirtybit = false; 
				}
				this.table.remove(bufDescr[minindex].pagenumber);
				
				if(emptyPage) {
					this.bufpool[minindex].copyPage(page);
				} else {
					Minibase.DiskManager.read_page(pageno, this.bufpool[minindex]);
				}
				page.setPage(this.bufpool[minindex]);
				
				this.bufDescr[minindex] = new Descriptor();
				this.bufDescr[minindex].pagenumber.copyPageId(pageno);
				this.bufDescr[minindex].pin_count++;
				this.bufDescr[minindex].usage++;
				table.put(this.bufDescr[minindex].pagenumber, minindex); 
				
			} else {
				if(emptyPage) {
					this.bufpool[bufsize].copyPage(page);
				} else {
					Minibase.DiskManager.read_page(pageno, this.bufpool[bufsize]);
				}
				page.setPage(this.bufpool[bufsize]);
				
				this.bufDescr[bufsize].pagenumber.copyPageId(pageno);
				this.bufDescr[bufsize].pin_count++;
				this.bufDescr[bufsize].usage++;
				
				this.table.put(this.bufDescr[bufsize].pagenumber, bufsize); 
				bufsize++;
			}
		}
		
	}
	
	/*
	 * Unpins a pinned page
	 */
	public void unpinPage(PageId pageno, boolean dirty) throws ChainException {
		if(table.contains(pageno) == false)
		{
			throw new HashEntryNotFoundException(null,"Hash Entry fails to exist");
		}
		
		int index = table.get(pageno);
		
		if(bufDescr[index].pin_count == 0) {
			throw new PageUnpinnedException(null, "Unpinned is trying to unpin itself");
		}
		
		bufDescr[index].dirtybit = dirty;
		bufDescr[index].pin_count--;
		
	}
	
	/*
	 * Free a page on the disk or on the bufferpool (deallocates)
	 */
	public void freePage(PageId globalPageId) throws ChainException { 
		if (!table.contains(globalPageId)) {
			Minibase.DiskManager.deallocate_page(globalPageId);
			return;
		}
		
		int index = table.get(globalPageId);
		if(bufDescr[index] != null) {
			if(bufDescr[index].pin_count > 0) {
				throw new PagePinnedException(null,"The page is pinned. Cannot free it.");
			}
			table.remove(globalPageId);
			bufDescr[index] = new Descriptor();
		}
		Minibase.DiskManager.deallocate_page(globalPageId);
	}
	
	/*
	 * Flushes a page (writes to disk)
	 */
	public void flushPage(PageId pageid) throws HashEntryNotFoundException {
		
		if (!table.contains(pageid)) {
			throw new HashEntryNotFoundException(null,"Hash Entry fails to exist");
		}
		
		try {
			
			int index = table.get(pageid);
			Minibase.DiskManager.write_page(this.bufDescr[index].pagenumber, this.bufpool[index]);
			this.bufDescr[index].dirtybit = false;
			
		} catch (InvalidPageNumberException e) {
			e.printStackTrace();
		} catch (FileIOException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Flush all pages if they are dirty
	 */
	public void flushAllPages() throws HashEntryNotFoundException {
		for(int i = 0; i < this.bufpool.length; i++) {
			if(this.bufDescr[i].dirtybit) {
				flushPage(this.bufDescr[i].pagenumber);
			}
		}
	}
	
	/*
	 * Returns the total number of buffer frames
	 */
	public int getNumBuffers() {
		return this.bufpool.length;
	}
	
	/*
	 * Return the number of unpinned frames in the buffer pool
	 */
	
	public int getNumUnpinned() { 
		int count = 0;
		for(int i = 0; i < bufpool.length; i++) {
			if(bufDescr[i].pin_count == 0) {
				count++;
			}
		}
		return count;
	}
	
	/*
	 * Returns unpinned index position for the replacement policy
	 */
	private ArrayList<Integer> getUnpinnedIndexes() {
		ArrayList<Integer> list = new ArrayList<Integer>();
		for(int i = 0; i < this.bufpool.length; i++) {
			if(this.bufDescr[i].pin_count == 0) {
				list.add((Integer)i);
			}
		}
		return list;
	}
}
