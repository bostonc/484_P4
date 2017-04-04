#include "LogMgr.h"
#include "assert.h"
#include <vector>
#include <sstream>

//LogMgr Private functions to implement

/*
   * Find the LSN of the most recent log record for this TX.
   * If there is no previous log record for this TX, return 
   * the null LSN.
   */
  int LogMgr::getLastLSN(int txnum) {
    int logtail_size = logtail.size();
    LogRecord* log_record;
    //find most recent log record for this TX
    for (int i = logtail_size - 1; i >= 0; i--) {
      log_record = logtail[i];
      assert(log_record);
      if (log_record->getTxID() == txnum) {
        return log_record->getLSN();
      }
    }
    return NULL_LSN;
  }

  /*
   * Update the TX table to reflect the LSN of the most recent
   * log entry for this transaction.
   */
  void LogMgr::setLastLSN(int txnum, int lsn) {
    //find last entry for this TX and erase it
    map <int, txTableEntry>::iterator it = tx_table.find(txnum);
    assert(it != tx_table.end());
    tx_table.erase(it);
    //insert new entry for this TX with given lsn
    txTableEntry update_entry(lsn, U);
    tx_table.insert(pair<txnum, update_entry>);
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

  vector<LogRecord*> stringToLRVector(string logstring) {
    //given to us in discussion slides
    vector<LogRecord*> result; 
    istringstream stream(logstring);
    string line; 
    while(getline(stream, line)) {
      LogRecord* lr = LogRecord::stringToRecordPtr(line);  
      result.push_back(lr); //slides said "Results" but I think it should be "result"
    }
    return result;
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











