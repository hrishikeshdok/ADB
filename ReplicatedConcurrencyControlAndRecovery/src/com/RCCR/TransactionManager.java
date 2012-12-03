package com.RCCR;
import java.util.*;


public class TransactionManager
{
	static ArrayList<Transaction> transactionList = new ArrayList<Transaction>();//keeps track of active transactions
	static Site sites[] = new Site[11];
	static boolean siteStatus[] = new boolean[11];//true if up, false if down
	static operation messageArray[] = new operation[11];//one for each site
	static ArrayList<operation> waitQueue = new ArrayList<operation>();//wait queue of transaction operations
	static int count = 0;
	static ArrayList<Transaction> keepTrackOfTrans = new ArrayList<Transaction>();//keeps track of transactions to print summary at the end

	public static void begin(String transactionName, Boolean isReadOnly)
	{
		Transaction newTrans = new Transaction(transactionName, isReadOnly, count);
		if(isReadOnly)
			newTrans.transSummary.add(("Transaction " + transactionName + " has begun at time " + newTrans.startTime + " and is read only"));
		else
			newTrans.transSummary.add(("Transaction " + transactionName + " has begun at time " + newTrans.startTime));
		transactionList.add(newTrans);
		keepTrackOfTrans.add(newTrans);
	}

	public static void dump()
	{
		for(int i=1; i<=10; i++)
			sites[i].showVariables();
	}

	public static void dump(int siteName)
	{
		sites[siteName].showVariables();
	}

	public static void dump(String variableNameString)
	{
		int variableName = Integer.parseInt(variableNameString.substring(1));
		if(variableName % 2 == 0)
		{
			for(int i=1; i<=10; i++)
				sites[i].showVariable(variableName);
		}
		else
			sites[1+(variableName%10)].showVariable(variableName);
	}

	public static void R(String transactionName, int variableName)
	{
		getTransaction(transactionName).transSummary.add("Transaction " + transactionName + " wants to read Variable x" + variableName);
		if(getTransaction(transactionName) != null)
			addOperationToBuffer(new operation(variableName, 'R', getTransaction(transactionName), -9999), 'R');
		else
			getTransaction(transactionName).transSummary.add("Transaction " + transactionName + " has been aborted so cannot process operation");
	}

	public static void W(String transactionName, int variableName, int value)
	{
		getTransaction(transactionName).transSummary.add("Transaction " + transactionName + " wants to update value of Variable x" + variableName + " to " + value);
		if(getTransaction(transactionName) != null)
			addOperationToBuffer(new operation(variableName, 'W', getTransaction(transactionName), value), 'W');
		else
			getTransaction(transactionName).transSummary.add("Transaction " + transactionName + " has been aborted so cannot process operation");
	}

	public static void addOperationToBuffer(operation newOperation, char operationType)
	{
		int choice = -1;
		if(operationType == 'R')//for read operations
		{
			if(newOperation.variableName % 2 == 0)//for replicated variables
			{
				for(int i=1; i<=10; i++)
				{
					if(siteStatus[i])//site is up
					{
						if(newOperation.transactionName.readOnly)//for readonly transactions, no locks required
						{
							choice = i;
							break;
						}
						else
						{
							if(sites[i].variables[newOperation.variableName].valid)//site can receive requests and not in recovery phase
							{
								int readPos = sites[i].checkIfReadPossible(newOperation);
								if(readPos == 0)//check before so that transaction operation is not stuck : true then add older asking to wait queue
								{
									//read possible  with present read lock so send request to that site
									choice = i;
									break;
								}
								else if(readPos == 1)//older transaction but write lock present so add to wait queue
								{
									String toAdd = "Transaction " + newOperation.transactionName.transactionName + " is older than lock holder and has been added to Wait Queue";
									if(newOperation.transactionName.transSummary.isEmpty() || (!newOperation.transactionName.transSummary.isEmpty() && !newOperation.transactionName.transSummary.get(newOperation.transactionName.transSummary.size() - 1).equals(toAdd)));//if the transaction wasnt just added to wait queue
										newOperation.transactionName.transSummary.add(toAdd);
									waitQueue.add(newOperation);
									return;
								}
								else if(readPos == 2)//asking is younger so abort it
								{
									newOperation.transactionName.transSummary.add("Write lock by older transaction present on x" + newOperation.variableName + " so abort younger transaction " + newOperation.transactionName.transactionName);
									removeTransaction(newOperation.transactionName);
									return;
								}
								else//readPos = 3 same transaction wants to read its temporary stored value
								{
									newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " reads its temporary value of x" + newOperation.variableName + " as " + sites[i].dlm.lockInformation[newOperation.variableName].tempValue);///%%%%not good practice
									return;
								}
							}
						}
					}
				}
			}
			else//non-replicated odd variables
			{
				if(siteStatus[1+(newOperation.variableName%10)])//site is up but since non-replicated variable no need to do recovery for reads or writes
				{
					if(newOperation.transactionName.readOnly)
						choice = 1 + (newOperation.variableName % 10);
					else
					{
						int readPos = sites[1+(newOperation.variableName%10)].checkIfReadPossible(newOperation);

						if(readPos == 0)//check before so that transaction operation is not stuck : true then add older asking to wait queue
							choice = 1 + (newOperation.variableName % 10);
						else if(readPos == 1)//older transaction but write lock present so add to wait queue
						{
							newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " is older than lock holder and has been added to Wait Queue");
							waitQueue.add(newOperation);
							return;
						}
						else if(readPos == 2)//asking is younger so abort it
						{
							newOperation.transactionName.transSummary.add("Write lock by older transaction present on x" + newOperation.variableName + " so abort younger transaction " + newOperation.transactionName.transactionName);
							removeTransaction(newOperation.transactionName);
							return;
						}
						else//readPos = 3 same transaction wants to read its temporary stored value
						{
							newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " reads its temporary value of x" + newOperation.variableName + " as " + sites[1+(newOperation.variableName%10)].dlm.lockInformation[newOperation.variableName].tempValue);
							return;
						}
					}
				}
			}
			if(choice != -1)//some site possible
			{
				newOperation.transactionName.transSummary.add("Reading from site " + choice);
				newOperation.transactionName.accessedSites[choice] = true;//set that the transaction accesses the given site
				if(messageArray[choice] == null)//first operation for that site
					messageArray[choice] = newOperation;
				else
				{
					operation temp = messageArray[choice];
					while(temp.nextOperation != null)//travel down linked list until end of list reached
						temp = temp.nextOperation;
					temp.nextOperation = newOperation;
				}
			}
			else
			{
				newOperation.transactionName.transSummary.add("Site unavailable for variable x" + newOperation.variableName + " so added to wait Queue");
				waitQueue.add(newOperation);
			}
		}
		else//for write operations
		{
			int checkIfWritePos = checkForWriteLock(newOperation);
			if(checkIfWritePos == 0)
				writeLockVariable(newOperation);
			else if(checkIfWritePos == 1)
			{
				waitQueue.add(newOperation);
				newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " has been added to wait Queue for write lock");
			}
		}
	}

	public static void end(String transactionName)
	{
		if(getTransaction(transactionName) != null)
		{
			getTransaction(transactionName).transSummary.add("Transaction " + transactionName + " is trying to commit");
			if(checkIfCanCommit(transactionName))//checks if the transaction has accessed some site which is down
			{
				getTransaction(transactionName).transSummary.add("Transaction " + transactionName + " can commit");
				if(getTransaction(transactionName) != null)//if the transaction is still active
				{
					for(int i=1; i<=10; i++)
					{
						if(siteStatus[i])
						{
							sites[i].commit(transactionName);
						}
					}
				}
			}
		}
		//else
			//getTransaction(transactionName).transSummary.add("Transaction " + transactionName + " has already been aborted");
	}

	public static void fail(int siteName)
	{
		System.out.println("Site " + siteName + " fails");
		siteStatus[siteName] = false;
		sites[siteName].failure();
	}

	public static void recover(int siteName)
	{
		System.out.println("Site " + siteName + " recovers");
		siteStatus[siteName] = true;
		int i = 0;
		while(i<transactionList.size())//abort all transactions that had accessed that site
		{
			if(transactionList.get(i).accessedSites[siteName] && !transactionList.get(i).readOnly)//transaction has accessed that site and is not readOnly
			{
				transactionList.get(i).transSummary.add("Transaction " + transactionList.get(i).transactionName + " has accessed Site " + siteName + " and hence is aborted");
				removeTransaction(transactionList.get(i));
			}
			else
				i++;
		}

	}

	public static Transaction getTransaction(String transactionName)
	{
		Transaction temp = null;
		for(int i=0; i<transactionList.size(); i++)
		{
			if(transactionList.get(i).transactionName.equals(transactionName))
			{
				temp = transactionList.get(i);
				return temp;
			}
		}
		return temp;
	}

	public static void sendMessagesToSites()
	{
		for(int i=1; i<=10; i++)
		{
			if(messageArray[i] != null)
			{
				sites[i].newMessage(messageArray[i]);//message is collection of operations in the form of a linked list
				messageArray[i] = null;
				//sites[i].checkBuffer();
			}
		}
	}

	public static void checkBuffers()//one by one check the buffers of all sites
	{
		for(int i=1; i<=10; i++)
		{
			if(siteStatus[i] == true)//site is up
				sites[i].checkBuffer();
		}
	}

	public static void checkWaitQueue()//check if there are any operations that are waiting for a variable
	{
		//System.out.println("Checking Wait Queue of Transaction Manager");
		int waitQueueSize = waitQueue.size();//record size of waitQueue to prevent rechecking same operation if it gets re-added
		for(int i=0; i<waitQueueSize; i++)
		{
			operation current = waitQueue.get(0);
			if(getTransaction(current.transactionName.transactionName) != null)//transaction still active?
				addOperationToBuffer(current, current.operationType);
			waitQueue.remove(0);
		}
	}

	public static boolean checkIfCanCommit(String transactionName)//checks if given transaction can commit or not
	{
		Transaction currentTrans = getTransaction(transactionName);
		if(currentTrans.readOnly == true)//if readonly, can commit
			return true;
		for(int i=1; i<=10; i++)
		{
			if(currentTrans.accessedSites[i] == true)
			{
				if(siteStatus[i] == false)//site that was accessed by transaction is down
				{
					currentTrans.transSummary.add("Transaction " + transactionName + " has accessed Site " + i + " which is down and hence it cannot commit and is aborted");
					removeTransaction(getTransaction(transactionName));//abort the transaction
					return false;
				}

			}
		}
		for(int i=0; i<waitQueue.size(); i++)//check if any operation concerning that transaction is present in any site
		{
			if(waitQueue.get(i).transactionName.transactionName.equals(transactionName))
			{
				currentTrans.transSummary.add("Transaction " + transactionName + " cant commit as it is waiting for a lock");
				removeTransaction(getTransaction(transactionName));//abort the transaction
				return false;
			}
		}
		return true;//all sites that transaction accessed are up
	}

	public static int checkForWriteLock(operation newOperation)
	{
		//check all sites where copy of variable is present
		//return 0 if write possible with all variables unlocked so send the write request to that site
		//return 1 if write possible but older asking has to be added to wait queue
		//return 2 if write not possible and younger asking has to be aborted
		//return 3 if site accessed has failed so aborted transaction

		int writeOrWait = 0;//default 0 means all copies unlocked
		if(newOperation.variableName % 2 == 0)//even variables present on all sites
		{
			for(int i=1; i<=10; i++)
			{
				if(siteStatus[i])//the site is up and functioning
				{
					if(sites[i].checkIfOlder(newOperation) == false)//site is up and says that older transaction has lock on given variable and so abort asking transaction, this breaks on first rejection
					{
						newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " has been aborted as it is younger than lock holder");
						removeTransaction(newOperation.transactionName);
						return 2;
					}
					else
					{
						if(sites[i].isLocked(newOperation.variableName) != 'N')
						{
							if(!sites[i].oldestLockHolder(newOperation.variableName).equals(newOperation.transactionName.transactionName))//same transaction isnt holding a read lock
								writeOrWait = 1;
							else if(sites[i].noOfLockHolders(newOperation.variableName) > 1) //oldest lockholder is the same, check if other transactions are also not holding lock
								writeOrWait = 1;//others are also present so add to waitQueue
						}
					}
				}
				else//site is down so check if the transaction has read from it earlier in case have to abort
				{
					if(newOperation.transactionName.accessedSites[i])
					{
						newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " has accessed Site " + i + " which is down so it will be aborted");
						removeTransaction(newOperation.transactionName);
						return 3;
					}
				}
			}
		}
		else//odd variables present only on one site
		{
			if(siteStatus[1+(newOperation.variableName%10)])
			{
				if(sites[1+(newOperation.variableName%10)].checkIfOlder(newOperation) == false)//site is up and says that older transaction has lock on given variable and so abort asking transaction
				{
					newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " has been aborted as it is younger than lock holder");
					removeTransaction(newOperation.transactionName);
					return 2;
				}
				else
				{
					if(sites[1+(newOperation.variableName%10)].isLocked(newOperation.variableName) != 'N')
					{
						if(!sites[1+(newOperation.variableName%10)].oldestLockHolder(newOperation.variableName).equals(newOperation.transactionName.transactionName))//same transaction isnt holding a read lock
							writeOrWait = 1;
						else if(sites[1+(newOperation.variableName%10)].noOfLockHolders(newOperation.variableName) > 1) //oldest lockholder is the same, check if other transactions are also not holding lock
							writeOrWait = 1;//others are also present so add to waitQueue
					}
				}
			}
			else//site is down so check if the transaction has read from it earlier in case have to abort
			{
				if(newOperation.transactionName.accessedSites[1+(newOperation.variableName%10)])
				{
					newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " has accessed Site " + (1+(newOperation.variableName%10)) + " which is down so it will be aborted");
					removeTransaction(newOperation.transactionName);
					return 3;
				}
				else
					writeOrWait = 1;//site down so readd into wait queue
			}
		}
		//all sites have lockholders younger than asking transaction then 1 is returned, if all unlocked 0 is returned
		return writeOrWait;
	}

	public static void removeTransaction(Transaction transactionName)
	{
		//first remove transaction from every site
		for(int i=1; i<=10; i++)
		{
			sites[i].removeTransaction(transactionName);
		}
		//remove from list of active transactions
		for(int i=0; i<transactionList.size(); i++)
		{
			if(transactionList.get(i).transactionName.equals(transactionName.transactionName))
				transactionList.remove(i);
		}
	}

	public static void writeLockVariable(operation newOperation)//all sites have agreed on write lock. lock the variable if site is up
	{
		newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " changes temporary values of all copies of variable x" + newOperation.variableName + " to " + newOperation.value);
		if(newOperation.variableName % 2 == 0)//replicated variables
		{
			for(int i=1; i<=10; i++)
			{
				if(siteStatus[i])//site is up
				{
					if(!sites[i].variables[newOperation.variableName].valid)//site is in recovery mode and the variable hasnt been recovered
					{
						System.out.println("Recovering variable x" + newOperation.variableName + " on Site " + i);
						sites[i].recoverVariable(newOperation);//recover all copies of that variable
					}
					sites[i].abortTransaction(newOperation);//abort all transactions holding Locks on all copies
					newOperation.transactionName.accessedSites[i] = true;//record that the transaction accessed that site
					sites[i].lockVariable(newOperation);//also abort ALL previous lock holding transactions
				}
			}
		}
		else
		{
			sites[1+(newOperation.variableName%10)].abortTransaction(newOperation);//abort all transactions holding Locks on all copies
			newOperation.transactionName.accessedSites[1+(newOperation.variableName%10)] = true;
			sites[1+(newOperation.variableName%10)].lockVariable(newOperation);
		}
	}

	public static Variable askForAvailableSite(operation newOperation)//checks for any up site
	{
		for(int i=1; i<=10; i++)
		{
			if(siteStatus[i])
			{
				if(sites[i].variables[newOperation.variableName].valid)//if the copy of variable is available for reads
					return(sites[i].variables[newOperation.variableName]);
			}
		}
		return null;
	}

	public static void printTransactionSummary()//prints summary of each transaction
	{
		for(int i=0; i<keepTrackOfTrans.size(); i++)
			keepTrackOfTrans.get(i).printSummary();
	}

	public static void queryState()//prints current state of system
	{
		System.out.println("Printing State of the System");

		//print contents of message buffer
		System.out.println("Printing contents of the Message Buffer");
		for(int i=1; i<=10; i++)
		{
			if(siteStatus[i])
			{
				if(messageArray[i] != null)
				{
					System.out.println("For Site " + i);
					operation temp = messageArray[i];
					while(temp != null)
					{
						System.out.print(temp.operationType + "(" + temp.transactionName.transactionName + ",x" + temp.variableName);
						if(temp.operationType == 'W')
							System.out.println("," + temp.value + ")");
						else
							System.out.println(")");
						temp = temp.nextOperation;
					}
				}
			}
		}

		//print contents of waitQueue
		System.out.println("Printing contents of the Wait Queue");
		for(int i=0; i<waitQueue.size(); i++)
		{
			System.out.print(waitQueue.get(i).operationType + "(" + waitQueue.get(i).transactionName.transactionName + ",x" + waitQueue.get(i).variableName);
			if(waitQueue.get(i).operationType == 'W')
				System.out.println("," + waitQueue.get(i).value + ")");
			else
				System.out.println(")");
		}

		//print lock information and commited values for each site
		System.out.println("Printing Lock Information for each site : ");
		for(int i=1; i<=10; i++)
		{
			if(!siteStatus[i])
				System.out.println("Site " + i + " is down");
			sites[i].printLockInformation();
			dump(i);
		}
	}
}

