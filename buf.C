#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    
    bool set = false;
    int firstFrame = clockHand;
    while(set == false){
        advanceClock(); //Advance clock pointer
        BufDesc *frame = &bufTable[clockHand];
        if(frame->valid == true){ //Valid set? yes
            if(frame->refbit == true){//refBit set? yes
                frame->refbit = false; //Is this right way to change the var?
                continue;
            }
            else{//refBit set? no
                if(frame->pinCnt == 0){//page pinned? no
                    if(frame->dirty == true){//dirty bit set? yes
                        Status status = frame->file->writePage(frame->pageNo, hashTable->lookup(frame->file, frame->pageNo, frame->frameNo));
                        if(status != OK){
                            return UNIXERR;
                        }
                    }
                    else{//dirty bit set? no
                        frame->Set(frame->file, frame->pageNo);
                        set = true;
                    }
                }
                else{//page pinned? yes
                    if(clockHand == firstFrame){
                        return BUFFEREXCEEDED;
                    }
                    continue;
                } 

            }
        }
        else{ //Valid set? no
            //invoke set() on frame
            frame->Set(frame->file, frame->pageNo);
            set = true;
        }
    }

    return OK;


}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status == OK) {
        BufDesc *frame = &bufTable[frameNo];
        frame->refbit = true;
        frame->pinCnt += 1;
        page = frame;
    } else {
        int frameNo = 0;
        Status allocStatus = allocBuf(frameNo);
        if (allocStatus == BUFFEREXCEEDED) {
            return BUFFEREXCEEDED;
        }
        BufDesc *frame = &bufTable[frameNo];
        Status readPageStatus = file->readPage(PageNo, &(file + PageNo));
        if (readPageStatus == UNIXERR) {
            return UNIXERR;
        }
        Status hashTableStatus = hashTable->insert(file, PageNo, frameNo);
        if (hashTableStatus == HASHTBLERROR) {
            return HASHTBLERROR;
        }
        frame->Set(file, PageNo);
        page = &frame;
    }
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{





}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{

    const Status status = file->allocatePage(pageNo);
    if (status != Status::OK) {
        return UNIXERR;  // Return on failure
    }
    file->allocatePage(pageNo);
    int tempframe;
    const Status status1 = allocBuf(tempframe);
    if (status1 != Status::OK) {
        return BUFFEREXCEEDED;  // Return on failure
    }
    allocBuf(tempframe); 
    const Status status2 =  hashTable->insert(file, pageNo, tempframe); 
    if (status2 != Status::OK) {
        return HASHTBLERROR;  // Return on failure
    }
    hashTable->insert(file, pageNo, tempframe);
    
    bufTable->Set(file, pageNo); 

    return OK;
    
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


