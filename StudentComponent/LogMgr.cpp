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
	//find last begin checkpoint
	int log_size = log.size();
	int begin = 0;
	for (int i = log_size - 1; i >= 0; i--) {
		if (log[i]->getType() == BEGIN_CKPT) {
			begin = i;
			break;
		}
	}
	//find last end checkpoint
	int end = 0;
	for (int j = log_size - 1; j >= 0; j--) {
		if (log[j]->getType() == END_CKPT) {
			end = j;
			break;
		}
	}
	assert(end > begin); //possibly also check and do something if end != begin + 1
	//set dirty page table and transaction table to what they were at the end checkpoint
	dirty_page_table = log[end]->getDirtyPageTable();
	tx_table = log[end]->getDirtyPageTable();
	
	//after end, go through the rest of the log and update transaction table and dirty page table
	for (int i = end + 1; i < log_size; i++) {
		//if we find an end log record, remove that transaction from the transaction table
		if (log[i]->getType == END) {
			int tx = log[i]->getTxID();
			tx_table.erase(tx);
		}
		//if it's any other type of record, add that transaction to the transaction table if it's not already there
		else {
			int tx = log[i]->getTxID();
			if (tx_table.find(tx) == tx_table.end()) {
				//add the transaction to the table
				TxStatus status;
				if (log[i]->getType() == COMMIT) {
					status = C;
				}
				else {
					status = U;
				}
				txTableEntry new_tx(log[i]->getLSN(), status);
				tx_table.insert(pair<int, txTableEntry>(tx, new_tx));
			}
			else {
				//update the transaction in the table
				tx_table[tx].lastLSN = log[i]->getLSN();
				if (log[i]->getType() == COMMIT) {
					tx_table[tx].status = C;
				}
				else {
					tx_table[tx].status = U;
				}
			}
		}
	}
		

}

/*
* Run the redo phase of ARIES.
* If the StorageEngine stops responding, return false.
* Else when redo phase is complete, return true. 
*/
bool LogMgr::redo(vector <LogRecord*> log) {
	
	//Find oldest update in log(smallest recLSN) and start at that point in the log
	//	For each redoable record :
	//Is the page in dirty page table ?
	//	Is recLSN for the page is less than or equal to the LSN of the current record ?
	//	Is the pageLSN less than the LSN of the log record ?
	//	If it yes for all three, redo the record
	//	Remove committed transactions from table



    return false; //to compile
}

/*
* If no txnum is specified, run the undo phase of ARIES.
* If a txnum is provided, abort that transaction.
* Hint: the logic is very similar for these two tasks!
*/
void LogMgr::undo(vector <LogRecord*> log, int txnum) { //in declaration int txnum = NULL_TX
	//Undo all in the transaction table starting with the transaction with the largest LSN value in transaction table
	//	For each record :
	//If update :
	//Create a CLR record in the log.Add end record if undoNext is null
	//	Add prevLSN to set to undo
	//	If CLR :
	//If undoNextLSN is null, add end record to log
	//	Otherwise, add undoNextLSN to the set to undo

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
	//ANY CLR ENTRIES SHOULD BE CREATED IN UNDO, NOT HERE.....................
}

/*
* Write the begin checkpoint and end checkpoint
*/
void LogMgr::checkpoint() {
	//write begin checkpoint to the logtail
	int begin_lsn = se->nextLSN();
	LogRecord* begin_checkpoint = new LogRecord(begin_lsn, NULL_LSN, NULL_TX, BEGIN_CKPT);
	logtail.push_back(begin_checkpoint);
	
	//write end checkpoint to the logtail
	int end_lsn = se->nextLSN();
	ChkptLogRecord* end_checkpoint = new ChkptLogRecord(end_lsn, begin_lsn, NULL_TX, tx_table, dirty_page_table);
	logtail.push_back(end_checkpoint);
	
	//write end checkpoint to stable storage
	bool written = false;
	if (se->store_master(end_lsn)) {
		written = true;
	}
	assert(written);
	
	//flush the logtail up to and including begin checkpoint 
	flushLogTail(begin_lsn);
	
}

/*
* Commit the specified transaction.
*/
void LogMgr::commit(int txid) 
{ //see pg 574 of textbook edition 2
	//change status
	tx_table[txid].status = C;

	//write commit to log tail
	int newLSN = se->nextLSN();
	LogRecord* cRec = new LogRecord(newLSN, getLastLSN(txid), txid, COMMIT);
	logtail.push_back(cRec);
	setLastLSN(txid, newLSN);

    //write log tail to disk
	//include all records up to and including txid's lastLSN
	flushLogTail(newLSN);

	//write end record
	LogRecord* eRec = new LogRecord(se->nextLSN(), getLastLSN(txid), txid, END);
	logtail.push_back(eRec);

	//remove transaction from tx table
	tx_table.erase(txid);
}

/*
* A function that StorageEngine will call when it's about to 
* write a page to disk. 
* Remember, you need to implement write-ahead logging
*/
void LogMgr::pageFlushed(int page_id) 
{
	flushLogTail(se->getLSN(page_id));
	dirty_page_table.erase(page_id);
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
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) 
{
	//write update record
	int newLSN = se->nextLSN();
	UpdateLogRecord* uRec = new UpdateLogRecord(newLSN, getLastLSN(txid), txid, page_id, offset, oldtext, input);
	logtail.push_back(uRec);

	//update tx_table, add new entry if needed
	tx_table[txid].lastLSN = newLSN;
	tx_table[txid].status = U;

	//check for page table entry, add one if needed.
	try
	{
		dirty_page_table.at(page_id);
	}
	catch (const std::out_of_range& e)
	{
		dirty_page_table[page_id] = newLSN;
	}
    return newLSN;
}

/*
* Sets this.se to engine. 
*/
void LogMgr::setStorageEngine(StorageEngine* engine) 
{
	this->se = engine;
}











