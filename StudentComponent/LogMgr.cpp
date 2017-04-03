#include "LogMgr.h"

//LogMgr Private functions to implement

/*
   * Find the LSN of the most recent log record for this TX.
   * If there is no previous log record for this TX, return 
   * the null LSN.
   */
  int LogMgr::getLastLSN(int txnum) {
     //TODO
     return 0; //to compile
  }

  /*
   * Update the TX table to reflect the LSN of the most recent
   * log entry for this transaction.
   */
  void LogMgr::setLastLSN(int txnum, int lsn) {
     //TODO
  }

  /*
   * Force log records up to and including the one with the
   * maxLSN to disk. Don't forget to remove them from the
   * logtail once they're written!
   */
  void LogMgr::flushLogTail(int maxLSN) {
     //TODO
  }

/* 
   * Run the analysis phase of ARIES.
   */
  void LogMgr::analyze(vector <LogRecord*> log) {
     //TODO
  }

  /*
   * Run the redo phase of ARIES.
   * If the StorageEngine stops responding, return false.
   * Else when redo phase is complete, return true. 
   */
  bool LogMgr::redo(vector <LogRecord*> log) {
    //TODO
      return false; //to compile
  }

  /*
   * If no txnum is specified, run the undo phase of ARIES.
   * If a txnum is provided, abort that transaction.
   * Hint: the logic is very similar for these two tasks!
   */
  void LogMgr::undo(vector <LogRecord*> log, int txnum) { //in declaration int txnum = NULL_TX
     //TODO
  }




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
     return 0; //to comple
  }

  /*
   * Sets this.se to engine. 
   */
  void LogMgr::setStorageEngine(StorageEngine* engine) {
     //TODO
  }











