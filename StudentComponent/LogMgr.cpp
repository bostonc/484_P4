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
int LogMgr::getLastLSN(int txnum) 
{	
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
void LogMgr::setLastLSN(int txnum, int lsn) 
{	
	//find entry for this TX and set lsn	
	tx_table[txnum].lastLSN = lsn;
}

/*
* Force log records up to and including the one with the
* maxLSN to disk. Don't forget to remove them from the
* logtail once they're written!
*/
void LogMgr::flushLogTail(int maxLSN) 
{	
	//write log records to disk
	for (int i = 0; i <= maxLSN; i++) {
		string log_record = logtail[i]->toString();
		this->se->updateLog(log_record);
	}
	//remove the records from logtail
	logtail.erase(logtail.begin(), logtail.begin() + maxLSN); //OFF BY ONE????
}

/* 
* Run the analysis phase of ARIES.
*/
void LogMgr::analyze(vector <LogRecord*> log) {
    //TODO
    
	//determine where in the log to start the Redo pass
    
	//determine which pages may have been dirty at the time of the crash -> fill dirty_page_table
    
	//identify transactions that were active at the time of the crash -> fill tx_table
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

vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
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
void LogMgr::abort(int txid) 
{
	undo(logtail, txid);
	//ANY CLR ENTRIES SHOULD BE HANDLED IN UNDO, NOT HERE.....................
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
	this->se = engine;
}











