#include "LogMgr.h"

//LogMgr Private functions to implement




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











