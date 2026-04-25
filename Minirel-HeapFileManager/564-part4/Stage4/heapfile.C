#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status == OK)
    {
        // file already exists
        return FILEEXISTS;
    }
    
    // create file
    if ((status = db.createFile(fileName)) != OK)
        return status;
    
    // open new file to initialize it
    if ((status = db.openFile(fileName, file)) != OK)
        return status;

    // allocate the header page
    if ((status = bufMgr->allocPage(file, hdrPageNo, newPage)) != OK)
        return status;

    // cast to FileHdrPage and initialize the fields
    hdrPage = (FileHdrPage*) newPage;
    strcpy(hdrPage->fileName, fileName.c_str());
    hdrPage->recCnt = 0;
    hdrPage->pageCnt = 1; 
        
    // allocate first data page
    if ((status = bufMgr->allocPage(file, newPageNo, newPage)) != OK)
    {
            bufMgr->unPinPage(file, hdrPageNo, true);
            return status;
    }

    // initialize data page contents
    newPage->init(newPageNo);
    hdrPage->firstPage = newPageNo;
    hdrPage->lastPage = newPageNo;
    hdrPage->pageCnt = 2;
        
    // unpin header page
    if ((status = bufMgr->unPinPage(file, hdrPageNo, true)) != OK)
    
        return status;
    
    // unpin data page
    if ((status = bufMgr->unPinPage(file, newPageNo, true)) != OK)
        return status;
    
    // close file
    if ((status = db.closeFile(file)) != OK)
        return status;
    
    return OK;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // get page number of header
         if ((status = filePtr->getFirstPage(headerPageNo)) == OK)
         {
            // read header page into buffer pool
            if ((status = bufMgr->readPage(filePtr, headerPageNo, pagePtr)) == OK)
            {
                // cast to FileHdrPage
                headerPage = (FileHdrPage*) pagePtr;
                hdrDirtyFlag = false;

                // pin first data page
                curPageNo = headerPage->firstPage;
                if ((status = bufMgr->readPage(filePtr, curPageNo, pagePtr)) == OK)
                {
                    curPage = pagePtr;
                    curDirtyFlag = false;
                    curRec = NULLRID;
                    returnStatus = OK;
                    return;
                }
            }
         }
    }

    cerr << "open of heap file failed\n";
	returnStatus = status;
	return;
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   // record is already on pinned page
    if (curPage != NULL && curPageNo == rid.pageNo)
   {
        status = curPage->getRecord(rid, rec);
        curRec = rid;
        return status;
   }

   // wrong or no page page pinned, unpin current page first
   if (curPage != NULL)
   {
        if ((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK)
        {
            return status;
        }
        curPage = NULL;
   }

   // read and pin page
   if ((status = bufMgr->readPage(filePtr, rid.pageNo, curPage)) != OK)
   {
        return status;
   }

   // bookkeeping
   curPageNo = rid.pageNo;
   curDirtyFlag = false;

   // get the record from the page
   if ((status = curPage->getRecord(rid, rec)) != OK)
    {
          return status;
    }

    curRec = rid;
    return OK;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status  status = OK;
    RID     nextRid;
    int     nextPageNo;
    Record  rec;

    // first call, pin first page 
    if (curPage == NULL)
    {
        if ((status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage)) != OK)
            return status;
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
        curRec = NULLRID;
    }

    while (true)
    {
        // see if we need first record or next
        if (curRec.pageNo == NULLRID.pageNo && curRec.slotNo == NULLRID.slotNo)
        {
            // Get the first record on the current page
            status = curPage->firstRecord(curRec);
        }
        else
        {
            // Get the next record on the current page
            status = curPage->nextRecord(curRec, nextRid);
            if (status == OK)
                curRec = nextRid;
        }

        // Handle end of page or no records
        if (status == NORECORDS || status == ENDOFPAGE)
        {
            // Try to move to the next page
            if ((status = curPage->getNextPage(nextPageNo)) != OK)
                return status;

            if (nextPageNo < 0)
            {
                // No more pages in the file
                return FILEEOF;
            }

            // Unpin the current page and read the next page
            if ((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK)
                return status;

            if ((status = bufMgr->readPage(filePtr, nextPageNo, curPage)) != OK)
                return status;

            curPageNo = nextPageNo;
            curDirtyFlag = false;
            curRec = NULLRID;
            continue;
        }

        // error handling
        if (status != OK)
            return status;

        // get the actual record data
        if ((status = curPage->getRecord(curRec, rec)) != OK)
            return status;

        // return iff satisfies scan
        if (matchRec(rec))
        {
            outRid = curRec;
            return OK;
        }

        // continue loop
    }
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*    newPage;
    int      newPageNo;
    Status   status, unpinstatus;
    RID      rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // load last page if nothing is pinned or we're on different page
    if (curPage == NULL || curPageNo != headerPage->lastPage)
    {
        curPageNo = headerPage->lastPage;
        if ((status = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK)
            return status;
        curDirtyFlag = false;
    }

    // try to insert into last page
    status = curPage->insertRecord(rec, rid);

    if (status == OK)
    {
        // if insert went good, bookkeeping
        curDirtyFlag = true;
        outRid = rid;
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        return OK;
    }
        
    // current page is full, alloc new page
    if ((status = bufMgr->allocPage(filePtr, newPageNo, newPage)) != OK)
        return status;

    newPage->init(newPageNo);

    // link last page to new page
    if ((status = curPage->setNextPage(newPageNo)) != OK)
    {
        bufMgr->unPinPage(filePtr, newPageNo, false);
        return status;
    }
    curDirtyFlag = true;

    // unpin old last page
    if ((unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK)
        return unpinstatus;
            
    // make new page current page
    curPage = newPage;
    curPageNo = newPageNo;
    curDirtyFlag = false;

    // update header
    headerPage->lastPage = newPageNo;
    headerPage->pageCnt++;
    hdrDirtyFlag = true;

    // insert into new page
    if ((status = curPage->insertRecord(rec, rid)) != OK)
        return status;

    // bookkeeping
    curDirtyFlag = true;
    outRid = rid;
    headerPage->recCnt++;
    hdrDirtyFlag = true;

    return OK;
}
        
    


    
    
    
    

    


