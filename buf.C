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
    for (int i = 0; i < numBufs * 2; i++){
        advanceClock(); //Advance clock pointer
        BufDesc *potentialFrame = &bufTable[clockHand];
        if(potentialFrame->valid == true){ //Valid set? yes
            if(potentialFrame->refbit == true){//refBit set? yes
                potentialFrame->refbit = false; //Is this right way to change the var?
                continue;
            }
            else{//refBit set? no
                if(potentialFrame->pinCnt <= 0){//page pinned? no
                    if(potentialFrame->dirty == true){//dirty bit set? yes
                        //How to get exact page from a file?
                        Status status = potentialFrame->file->writePage(potentialFrame->pageNo, &(bufPool[clockHand]));
                        if(status != OK){
                            return status;
                        }
                        potentialFrame->dirty = false;
                    }

                    potentialFrame->Set(potentialFrame->file, potentialFrame->pageNo);

                    Status remStatus = hashTable->remove(potentialFrame->file, potentialFrame->pageNo);
                    if(remStatus != OK){
                        return remStatus;
                    }

                    potentialFrame->Clear();

                    frame = clockHand;
                    return OK;

                }
                else{//page pinned? yes
                    // if(clockHand == firstFrame){
                    //     return BUFFEREXCEEDED;
                    // }
                    continue;
                } 
            }
        }
        else{ //Valid set? no
            //invoke set() on frame
            potentialFrame->Set(potentialFrame->file, potentialFrame->pageNo);
            frame = clockHand;
            return OK;
        }
    }

    return BUFFEREXCEEDED;
}

/**
 * Read page in buffer pool and output a pointer to its data.
 * If the page is not in the buffer pool currently, load page from disk
 * into buffer and output a pointer to its data.
 * 
 * Input
 * file - file pointer containing page to read
 * PageNo - page number within file of page to read
 * 
 * Output
 * page - output pointer to the desired page 
 * 
 * return OK on success, and either UNIXERR, BUFFEREXCEEDED, or HASHTBLERROR on error.
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = -999;
    Status status = hashTable->lookup(file, PageNo, frameNo); // check if page in buffer

    if (status == OK) { // page in buffer?
        // mark frame as having been referenced recently
        BufDesc *frame = &bufTable[frameNo];
        frame->refbit = true;
        frame->pinCnt += 1;
        page = &bufPool[frameNo]; // output pointer to page
    } else { // page not in buffer?
        int frameNo = -99999;
        Status allocStatus = allocBuf(frameNo); // allocate frame in buffer to store page
        if (allocStatus != OK) return allocStatus; // return UNIXERR if something went wrong
        BufDesc *frame = &bufTable[frameNo];
        Status readPageStatus = file->readPage(PageNo, &(bufPool[frameNo])); // read page from disk and insert into frame
        if (readPageStatus != OK) return readPageStatus; // return BUFFEREXCEEDED if all pages pinned
        Status hashTableStatus = hashTable->insert(file, PageNo, frameNo); // insert entry into hashtable
        if (hashTableStatus != OK) return hashTableStatus; // return HASHTABLEERROR if something went wrong
        frame->Set(file, PageNo); // set frame with new page
        page = &(bufPool[frameNo]); // output pointer to page
    }
    return OK;
}

/**
 * Unpins a desired page. 
 * If page is dirty, mark the frame as such, then unpin it.
 * 
 * Input
 * file - file pointer containing page to unpin
 * PageNo - page number within file of page to unpin
 * dirty - if page to unpin is dirty
 * 
 * return OK on success, and either HASHNOTFOUND or PAGENOTPINNED on error.
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
                   const bool dirty) 
{
   int frameNo = -999999;
   Status status = hashTable->lookup(file, PageNo, frameNo); // is the page in the buffer?
   if (status != OK) {return status;} // if not, return HASHNOTFOUND
   if (bufTable[frameNo].pinCnt == 0) {return PAGENOTPINNED;} // page to unpin is not pinned. return PAGENOTPINNED
   if (dirty) {bufTable[frameNo].dirty = true;} // if page is dirty, mark it as such
   bufTable[frameNo].pinCnt = bufTable[frameNo].pinCnt - 1; // decrement the pincount

   return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    const Status status = file->allocatePage(pageNo);
    
    if (status != Status::OK) {
        return UNIXERR;  // Return on failure
    }
    int tempframe = -999999;
    const Status status1 = allocBuf(tempframe);
    if (status1 != Status::OK) {
        if (status1 == BUFFEREXCEEDED) return BUFFEREXCEEDED;
        else if (status1 == UNIXERR) return UNIXERR;  // Return on failure
        return status1;
    }
    const Status status2 =  hashTable->insert(file, pageNo, tempframe); 
    if (status2 != Status::OK) {
        return HASHTBLERROR;  // Return on failure
    }
    bufTable[tempframe].Set(file, pageNo); 
    page = &(bufPool[tempframe]);

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

