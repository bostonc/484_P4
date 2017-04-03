//LogMgr Public functions to implement, there are also private functions 

/*
   * Abort the specified transaction.
   * Hint: you can use your undo function
   */
  void abort(int txid);

  /*
   * Write the begin checkpoint and end checkpoint
   */
  void checkpoint();

  /*
   * Commit the specified transaction.
   */
  void commit(int txid);

  /*
   * A function that StorageEngine will call when it's about to 
   * write a page to disk. 
   * Remember, you need to implement write-ahead logging
   */
  void pageFlushed(int page_id);

  /*
   * Recover from a crash, given the log from the disk.
   */
  void recover(string log);

  /*
   * Called by StorageEngine whenever an update is called
   * LogMgr should update tables if required and return the LSN of the action performed
   */
  int write(int txid, int page_id, int offset, string input, string oldtext);

  /*
   * Sets this.se to engine. 
   */
  void setStorageEngine(StorageEngine* engine);
