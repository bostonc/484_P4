#include "LogMgr.h"
#include "assert.h"
#include <queue>
#include <vector>
#include <sstream>
#include <string>
//#include <i1o2s3t4r5e6am> //DELETE BEFORE SUBMITTING!!!

//Helper Functions

//returns pointer to a log record, given a log vector & a LSN
//scans forward
LogRecord* getRecordFromLSN_forward(vector <LogRecord*> log, int lsn)
{
	for (int i = 0; i < (int)log.size(); ++i)
	{
		if (log[i]->getLSN() == lsn) return log[i];
	}
	return nullptr;
}

//returns pointer to a log record, given a log vector & a LSN
//scans backward
LogRecord* getRecordFromLSN_backward(vector <LogRecord*> log, int lsn)
{
	for (int i = log.size() - 1; i >= 0; --i)
	{
		if (log[i]->getLSN() == lsn) return log[i];
	}
	return nullptr;
}

//returns pointer to a log record, given a sorted log vector and a tx_id
LogRecord* getLatestRecordFromTxId(vector <LogRecord*> log, int txid)
{
	for (int i = log.size() - 1; i >= 0; --i)
	{
		if (log[i]->getTxID() == txid) return log[i];
	}
	return nullptr;
}

//returns the index of a log record, given a log and a LSN
int findRecordIdxWithLSN(vector <LogRecord*> log, int lsn)
{
	assert(log.size() >= 0);
	if (log.size() == 1 && log[0]->getLSN() > lsn) return 0;
	for (int i = 0; i < (int)log.size(); ++i)
	{
		if (log[i]->getLSN() == lsn) return i;
		if (log[i]->getLSN() > lsn) return i - 1;
	}
	return -1;
}


//LogMgr Private functions to implement

/*
   * Find the LSN of the most recent log record for this TX.
   * If there is no previous log record for this TX, return 
   * the null LSN.
   */
int LogMgr::getLastLSN(int txnum) 
{	
// 	int logtail_size = logtail.size();
// 	LogRecord* log_record;
// 	//find most recent log record for this TX
// 	for (int i = logtail_size - 1; i >= 0; i--) {
// 		log_record = logtail[i];
// 		assert(log_record);
// 		if (log_record->getTxID() == txnum) {
// 			return log_record->getLSN();
// 		}
// 	}
// 	return NULL_LSN;
	
	if (tx_table.find(txnum) != tx_table.end()) {
		//it's in tx table
		return tx_table[txnum].lastLSN;
	}
	else {
		//it's not in tx table
		return NULL_LSN;
	}
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
	//int begin_lsn = logtail[0]->getLSN();
	//int lsn_diff = logtail[1]->getLSN() - begin_lsn;
	//int loop_end = (maxLSN - begin_lsn) / lsn_diff;  //OFF BY ONE????
	//for (int i = 0; i <= loop_end; i++) {
	//	string log_record = logtail[i]->toString();
	//	this->se->updateLog(log_record);
	//}
	////remove the records from logtail
	//logtail.erase(logtail.begin(), logtail.begin() + loop_end);


	int debugCount = 0; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	int debugBeforeSize = (int)logtail.size(); //DELETE ME!!!!!!!!!!!!!!!!!!!!!
	//find idx of record with maxLSN
	int stopIdx = findRecordIdxWithLSN(logtail, maxLSN); //OFF BY ONE???
	//write records to disk
	for (int i = 0; i <= stopIdx; ++i)
	{
		se->updateLog(logtail[i]->toString());
		debugCount++;
	}
	//erase written records from logtail
	logtail.erase(logtail.begin(), logtail.begin() + stopIdx + 1);
	//c123out << "Logtail flushed. Before: " << debugBeforeSize
		//<< ", After: " << logtail.size() << ", Intended: " 
		//<< debugCount << ", Actual: " << debugBeforeSize - logtail.size() 
		//<< endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
}

/* 
* Run the analysis phase of ARIES.
*/
void LogMgr::analyze(vector <LogRecord*> log) {
	//c123out << "==Begin Analysis==" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!
	//find last begin checkpoint
	int log_size = log.size();
	int begin = -1;
	int end = -1;
	int begin_lsn = se->get_master();
	//check if there's been a checkpoint made yet
	if (begin_lsn!= -1) {
		for (int i = log_size - 1; i >= 0; i--) {
			if (log[i]->getLSN() == begin_lsn) {
				begin = i;
				break;
			}
		}
		assert(begin != -1);
		//find last end checkpoint
		for (int j = log_size - 1; j >= begin; j--) {
			if (log[j]->getType() == END_CKPT) {
				end = j;
				break;
			}
		}
		assert(end != -1); 
		
		//set dirty page table and transaction table to what they were at the end checkpoint
		ChkptLogRecord* log_end = dynamic_cast<ChkptLogRecord*>(log[end]);
		dirty_page_table = log_end->getDirtyPageTable();
		tx_table = log_end->getTxTable();
	}
	else {
		//clear transaction table and dirty pages table
		tx_table.clear();
		dirty_page_table.clear();
	}
	
	//after end, go through the rest of the log and update transaction table and dirty page table
	for (int i = begin + 1; i < log_size; i++) { //will start at 0 if no checkpoint has been made yet
		//if we find an end log record, remove that transaction from the transaction table
		if (log[i]->getType() == END) {
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
		//if its a redoable log record with a page that's not in the dirty page table, add it
		//redoable log record = UPDATE, CLR, is that it?
		if (log[i]->getType() == UPDATE) {
			UpdateLogRecord* log_i = dynamic_cast<UpdateLogRecord*>(log[i]);
			int page_id = log_i->getPageID();
			if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
				//add the page to the table
				int lsn = log_i->getLSN();
				dirty_page_table.insert(pair<int, int>(page_id, lsn));
			}
		}
		else if (log[i]->getType() == CLR) {
			CompensationLogRecord* log_i = dynamic_cast<CompensationLogRecord*>(log[i]);
			int page_id = log_i->getPageID();
			if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
				//add the page to the table
				int lsn = log_i->getLSN();
				dirty_page_table.insert(pair<int, int>(page_id, lsn));
			}
		}
	}
		
	//c123out << "==End Analysis==" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!
}

/*
* Run the redo phase of ARIES.
* If the StorageEngine stops responding, return false.
* Else when redo phase is complete, return true. 
*/
bool LogMgr::redo(vector <LogRecord*> log) 
{	
	//c123out << "==Begin Redo==" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!

	//Find oldest update in log(smallest recLSN) and start at that point in the log
	//	For each redoable record :
	//Is the page in dirty page table ?
	//	Is recLSN for the page is less than or equal to the LSN of the current record ?
	//	Is the pageLSN less than the LSN of the log record ?
	//	If it yes for all three, redo the record
	//	Remove committed transactions from table
	
	assert((int)log.size() > 0);

	map<int, int>* dpt = &dirty_page_table;
	int startingLSN = NULL_LSN;
	int startingLogIdx = -1;

	//find smallest recLSN in dirty_page_table
	for (map<int, int>::iterator it = dpt->begin(); it != dpt->end(); ++it)
	{
		if (startingLSN == NULL_LSN) startingLSN = it->second; //should only happen once
		if (startingLSN > it->second) startingLSN = it->second;		
	}

	//find vector index of log record with startingLSN
	for (int i = 0; i < (int)log.size(); ++i)
	{
		if (log[i]->getLSN() == startingLSN)
		{
			startingLogIdx = i;
			break;
		}
	}

	//scan forward starting with startingLogIdx, REDO where necessary
	for (int i = startingLogIdx; i < (int)log.size(); ++i)
	{
		//check type, find affected page if applicable
		int affectedPage = -1;
		UpdateLogRecord* updateLog = nullptr;
		CompensationLogRecord* clr = nullptr;
		switch (log[i]->getType())
		{
		case UPDATE:
			updateLog = dynamic_cast<UpdateLogRecord*>(log[i]);
			affectedPage = updateLog->getPageID();
			break;
		case CLR: 
			clr = dynamic_cast<CompensationLogRecord*>(log[i]);
			affectedPage = clr->getPageID();
			break;
		//case COMMIT:
		//case ABORT:
		//	//change status/write end record
		//case END:
		//	//remove from tx table
		default:
			continue;
		}

		//is affected page in the DPT?
		try	{ dpt->at(affectedPage); }
		catch (const std::out_of_range& e) { continue; }

		//is dpt recLSN greater than the LSN of the log record being checked?
		if (dpt->at(affectedPage) > log[i]->getLSN()) continue;

		//is pageLSN greater than or equal to the LSN of the log record being checked?
		if (se->getLSN(affectedPage) >= log[i]->getLSN()) continue;

		//get redo info
		int off = -1;
		string text = "";
		if (updateLog)
		{
			off = updateLog->getOffset();
			text = updateLog->getAfterImage();
		}
		else //CLR
		{
			off = clr->getOffset();
			text = clr->getAfterImage();
		}

		//attempt REDO
		if (!se->pageWrite(affectedPage, off, text, log[i]->getLSN())) return false;
	}//end for loop (log scan)	

	//MIGHT NEED TO BE DONE IN MAIN SCAN.........................................!
	vector<int> toBeRemoved;
	//write end records for all commited tx in the tx_table and erase from table
	for (map<int, txTableEntry>::iterator it = tx_table.begin(); it != tx_table.end(); ++it)
	{
		if (it->second.status == C)
		{
			LogRecord* endRec = new LogRecord(se->nextLSN(), it->second.lastLSN, it->first, END);
			logtail.push_back(endRec);
			//c123out << "END written." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!
			//tx_table.erase(it->first);
			toBeRemoved.push_back(it->first);
		}
	}
	for (int i = 0; i < (int)toBeRemoved.size(); ++i)
	{
		tx_table.erase(toBeRemoved[i]);
	}
	toBeRemoved.clear();

	//c123out << "==End Redo==" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    return true;
}

/*
* If no txnum is specified, run the undo phase of ARIES.
* If a txnum is provided, abort that transaction.
* Hint: the logic is very similar for these two tasks!
*/
void LogMgr::undo(vector <LogRecord*> log, int txnum) //declared: txnum = NULL_TX
{ 
	//c123out << "==Begin Undo==" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!

	//Undo all in the transaction table starting with the transaction with the largest LSN value in transaction table
	//	For each record :
	//If update :
	//Create a CLR record in the log.Add end record if undoNext is null
	//	Add prevLSN to set to undo
	//	If CLR :
	//If undoNextLSN is null, add end record to log
	//	Otherwise, add undoNextLSN to the set to undo

	//create ToUndo
	priority_queue<int> ToUndo;
	LogRecord* currLog;
	int abortLSN = -1;
	if (txnum == NULL_TX)
	{	//initialize for undo phase
		for (map<int, txTableEntry>::iterator it = tx_table.begin(); it != tx_table.end(); ++it)
		{
			ToUndo.push(it->second.lastLSN);
		}
	}
	else
	{	//initialize for single abort.
		//1) if it's in the tx_table, use that. 
		//3) Otherwise, check the log
		log.insert(log.end(), logtail.begin(), logtail.end());
		try
		{	//1
			abortLSN = tx_table.at(txnum).lastLSN;			
		}
		catch (const std::out_of_range& e)
		{	
			//2
			currLog = getLatestRecordFromTxId(log, txnum);
		}
		ToUndo.push(abortLSN);
	}

	//scan backwards from startingLSN
	while (!ToUndo.empty())
	{
		//find log record with LSN == largest LSN of ToUndo
		currLog = getRecordFromLSN_backward(log, ToUndo.top());

		switch (currLog->getType())
		{
		case CLR:
		{
			CompensationLogRecord* curr_clr = dynamic_cast<CompensationLogRecord*>(currLog);
			if (curr_clr->getUndoNextLSN() != NULL_LSN)
			{
				ToUndo.push(curr_clr->getUndoNextLSN());
				ToUndo.pop();
				continue;
			}
			//write end record
			LogRecord* endRec = new LogRecord(se->nextLSN(), curr_clr->getLSN(), curr_clr->getTxID(), END);
			logtail.push_back(endRec);
			tx_table[endRec->getTxID()].lastLSN = endRec->getLSN();
			//c123out << "END written." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!!
			ToUndo.pop();
			//erase transaction from tx table after END
			tx_table.erase(curr_clr->getTxID());
			continue;
		}			
		case UPDATE:
		{
			//write CLR
			UpdateLogRecord* curr_uplr = dynamic_cast<UpdateLogRecord*>(currLog);
			int nextLSN = se->nextLSN();
			//CompensationLogRecord * clr = new CompensationLogRecord(nextLSN, curr_uplr->getLSN(),
			//	curr_uplr->getTxID(), curr_uplr->getPageID(), curr_uplr->getOffset(),
			//	curr_uplr->getBeforeImage(), curr_uplr->getprevLSN()); //MIGHT HAVE BUGS
			CompensationLogRecord * clr = new CompensationLogRecord(nextLSN, getLastLSN(curr_uplr->getTxID()),
				curr_uplr->getTxID(), curr_uplr->getPageID(), curr_uplr->getOffset(),
				curr_uplr->getBeforeImage(), curr_uplr->getprevLSN());
			logtail.push_back(clr);
			tx_table[clr->getTxID()].lastLSN = clr->getLSN();
			//c123out << "CLR written." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!

			//undo action
			se->pageWrite(curr_uplr->getPageID(), curr_uplr->getOffset(), curr_uplr->getBeforeImage(), nextLSN);

			//write END and continue if prevLSN is null
			if (curr_uplr->getprevLSN() == NULL_LSN)
			{
				LogRecord* eRec = new LogRecord(se->nextLSN(), nextLSN, curr_uplr->getTxID(), END);
				logtail.push_back(eRec);
				tx_table[eRec->getTxID()].lastLSN = eRec->getLSN();
				//c123out << "END written." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!!
				ToUndo.pop();
				//erase transaction from tx table after END
				tx_table.erase(curr_uplr->getTxID());
				continue;
			}
			break;
		}			
		default: 
			break;
		}
		ToUndo.pop();
		if (currLog->getprevLSN() != NULL_LSN) ToUndo.push(currLog->getprevLSN());
	} //end backwards scan (while loop)

	//c123out << "==End Undo==" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
	//c123out << "Abort called" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!!!!
	//ANY CLR ENTRIES SHOULD BE CREATED IN UNDO, NOT HERE.

	//write ABORT log
	int nextLSN = se->nextLSN();
	LogRecord* aRec = new LogRecord(nextLSN, tx_table[txid].lastLSN, txid, ABORT);
	logtail.push_back(aRec);
	tx_table[txid].lastLSN = nextLSN;

	undo(stringToLRVector(se->getLog()), txid);
	

	//Write END???????????????????????????????????????????????????????????????


}

/*
* Write the begin checkpoint and end checkpoint
*/
void LogMgr::checkpoint() {
	//c123out << "Writing checkpoint..." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!

	//write begin checkpoint to the logtail
	int begin_lsn = se->nextLSN();
	LogRecord* begin_checkpoint = new LogRecord(begin_lsn, NULL_LSN, NULL_TX, BEGIN_CKPT);
	logtail.push_back(begin_checkpoint);
	
	//write end checkpoint to the logtail
	int end_lsn = se->nextLSN();
	ChkptLogRecord* end_checkpoint = new ChkptLogRecord(end_lsn, begin_lsn, NULL_TX, tx_table, dirty_page_table);
	logtail.push_back(end_checkpoint);

	//flush the logtail
	//flushLogTail(begin_lsn); //at time of begin
	flushLogTail(end_lsn);
	
	//record master
	se->store_master(begin_lsn); //should be begin LSN

	//c123out << "Checkpoint written." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!
}

/*
* Commit the specified transaction.
*/
void LogMgr::commit(int txid) 
{ //see pg 574 of textbook edition 2
	//c123out << "Committing tx_" << txid << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!

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
	//c123out << "Commit of tx_" << txid << " finished." << endl; //DELETE ME!!!!!!!!!!!!
}

/*
* A function that StorageEngine will call when it's about to 
* write a page to disk. 
* Remember, you need to implement write-ahead logging
*/
void LogMgr::pageFlushed(int page_id) 
{
	//c123out << "Flushing Page " << page_id << endl; //DELETE ME!!!!!!!!!!!!!!!!!!
	flushLogTail(se->getLSN(page_id));
	dirty_page_table.erase(page_id);
}

/*
* Recover from a crash, given the log from the disk.
*/
void LogMgr::recover(string log) 
{
	//c123out << "#### Begin recovery ####" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!
	
	//convert to vector
	vector<LogRecord*> log_record = stringToLRVector(log);

	//analyze
	analyze(log_record);

	//redo
	if (!redo(log_record))
	{
		//c123out << "Redo couldn't finish!" << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!
		return;
	}

	//undo
	undo(log_record);

	//c123out << "#### Successful recovery. :) ####" << endl;
}

/*
* Called by StorageEngine whenever an update is called
* LogMgr should update tables if required and return the LSN of the action performed
*/
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) 
{
	//c123out << "Beginning update..." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!

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

	//c123out << "Update complete." << endl; //DELETE ME!!!!!!!!!!!!!!!!!!!!!!!
    return newLSN;	
}

/*
* Sets this.se to engine. 
*/
void LogMgr::setStorageEngine(StorageEngine* engine) 
{
	this->se = engine;
	//c123out << "INITIALIZED" << endl;
}











