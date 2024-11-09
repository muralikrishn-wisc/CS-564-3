
     for (int i = 0; i < numBufs * 2; i++) {
        advanceClock();
        if (bufTable[clockHand].valid) {
            if (bufTable[clockHand].refbit) {
                bufTable[clockHand].refbit = false;
                continue;
            } else {
                if (bufTable[clockHand].pinCnt > 0) {
                    continue;
                } else {
                    if (bufTable[clockHand].dirty) {
                        if (bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]) == OK) { // write to disk
                            bufTable[clockHand].dirty = false; // not dirty anymore
                        } else { // disk write failed
                            return UNIXERR;
                        }
                    }
                    
                    // invoke set and use frame
                    bufTable[clockHand].Set(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                    frame = clockHand;
                    
                    // remove entry from hash since the buf allocated has valid entry
                    hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                    bufTable[clockHand].Clear();
                    
                    return OK;
                }
            }
        } else {
            // invoke set and use frame
            bufTable[clockHand].Set(bufTable[clockHand].file, bufTable[clockHand].pageNo);
            frame = clockHand;
            return OK;
        }
    }

    // clock loop finished, buffer is full
    return BUFFEREXCEEDED;