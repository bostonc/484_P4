#include "LogMgr.h"

//LogMgr Private functions to implement

/*
   * Find the LSN of the most recent log record for this TX.
   * If there is no previous log record for this TX, return 
   * the null LSN.
   */
  int getLastLSN(int txnum);

  /*
   * Update the TX table to reflect the LSN of the most recent
   * log entry for this transaction.
   */
  void setLastLSN(int txnum, int lsn);

  /*
   * Force log records up to and including the one with the
   * maxLSN to disk. Don't forget to remove them from the
   * logtail once they're written!
   */
  void flushLogTail(int maxLSN);


//LogMgr Public functions to implement

/*
   * Abort the specified transaction.
   * Hint: you can use your undo function
   */
  void LogMgr::abort(int txid) {
    //TODO
  }

  /*
   * Write the begin checkpoint and end checkpoint
   */
  void LogMgr::checkpoint() {
    //TODO
  }

  /*
   * Commit the specified transaction.
   */
  void LogMgr::commit(int txid) {
      //TODO
  }

  /*
   * A function that StorageEngine will call when it's about to 
   * write a page to disk. 
   * Remember, you need to implement write-ahead logging
   */
  void LogMgr::pageFlushed(int page_id) {
      //TODO
  }

  /*
   * Recover from a crash, given the log from the disk.
   */
  void LogMgr::recover(string log) {
    //TODO
  }

  /*
   * Called by StorageEngine whenever an update is called
   * LogMgr should update tables if required and return the LSN of the action performed
   */
  int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
      //TODO
  }

  /*
   * Sets this.se to engine. 
   */
  void LogMgr::setStorageEngine(StorageEngine* engine) {
      //TODO
  }











