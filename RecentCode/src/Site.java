import java.io.*;
import java.util.*;
import java.sql.*;

class Lock
{
	boolean isLocked;
	char lockType;//can be R/W/N
	ArrayList<Transaction> transactionList = new ArrayList<Transaction>();
	int tempValue;//temporary changes made by transaction in write operation

	Lock()
	{
		isLocked = false;
		lockType = 'N';
		transactionList = new ArrayList<Transaction>();
		tempValue = -1;
	}

	public void lockVariable(operation newOperation)
	{
		isLocked = true;
		this.lockType = newOperation.operationType;
		if(newOperation.operationType == 'W')
		{
			this.tempValue = newOperation.value;
			transactionList.clear();//only one transaction can have write lock so just in case, delete all elements from list
			transactionList.add(newOperation.transactionName);
		}
		else//for read or no lock
		{
			int i = 0;
			while( i < transactionList.size() && transactionList.get(i).startTime < newOperation.transactionName.startTime)
				i++;
			transactionList.add(i, newOperation.transactionName);
		}
	}

	public void checkLocks(int variableName)//determining type of lock on variable, or if it is unlocked
	{
		if(isLocked == false)
			System.out.println("Variable x" + variableName + " is not Locked");
		else
		{
			if(lockType == 'R')
			{
				System.out.println("Variable x" + variableName + " is Read Locked by Transaction(s) : ");
				for(int i=0; i<transactionList.size(); i++)
					System.out.println(transactionList.get(i).transactionName);
			}
			else
				System.out.println("Variable x" + variableName + " is Write Locked by Transaction : " + transactionList.get(0).transactionName);
		}
	}

	public void clearLock()//reset lock
	{
		isLocked = false;
		lockType = 'N';
		transactionList.clear();
		tempValue = -1;
	}
}


class Variable
{
	int index;
	int value;
	int commitTime;
	boolean valid;//False if site recovers and no write takes place on the variable yet
	Variable prevVar;//previous commited copy of variable

	Variable(int index, int value, int count)
	{
		this.index = index;
		this.value = value;
   		commitTime = count;
   		valid = true;
   		prevVar = null;
	}

	void invalidate()
	{
		valid = false;
	}

	void validate()
	{
		valid = true;
	}
}

class operation//chain of operations
{
	int variableName;
	char operationType;// R/W
	Transaction transactionName;
	int value;//if a write operation
	operation nextOperation;
	boolean transactionActive;//determines if transaction is active and operation is to be executed or not

	operation()
	{}

	operation(int variableName, char operationType, Transaction transactionName, int value)
	{
		this.variableName = variableName;
		this.operationType = operationType;
		this.transactionName = transactionName;
		this.value = value;
		nextOperation = null;
		transactionActive = true;
	}
}

class Site
{
	int index;
	ArrayList<operation> messageBuffer;
	Variable variables[];
	DataLockManager dlm;

	Site(int index, int count)
	{
		this.index = index;
		messageBuffer = new ArrayList<operation>();
		variables = new Variable[21];
		for(int i=1; i<=20; i++)
		{
			if(i%2==0)
				variables[i] = new Variable(i, 10 * i, count);
			else
			{
				if(1+(i%10) == index)
				{
					variables[i] = new Variable(i, 10 * i, count);
				}
				else
					variables[i] = null;
			}
		}
		dlm = new DataLockManager();
	}

	public void checkBuffer()
	{
		dlm.checkBuffer();
	}

	public void showVariables()//shows values of all variable copies on this site
	{
		System.out.println("For Site " + index);
		for(int i=1; i<=20; i++)
		{
			if(variables[i] != null)
				System.out.println("variable x" + i + " = " + variables[i].value);
		}
	}

	public void showVariable(int variableName)//shows value of a particular variable copy on this site
	{
		System.out.println("Committed values at Site " + index + " x" + variableName + " = " + variables[variableName].value);
	}

	public char isLocked(int variableName)
	{
		return dlm.isLocked(variableName);
	}

	public void lockVariable(operation newOperation)
	{
		dlm.lockVariable(newOperation);
	}

	public void newMessage(operation newOperation)
	{
		messageBuffer.add(newOperation);
	}

	public void showOperations()
	{
		if(messageBuffer.isEmpty())
			System.out.println("Message Buffer of Site " + index + " is empty");
		else
		{
			for(int i=0; i<messageBuffer.size(); i++)
			{
				operation temp = messageBuffer.get(i);
				do
				{
					System.out.println("Transaction " + temp.transactionName.transactionName + " performs operation " + temp.operationType + " on variable x" + temp.variableName);
					temp = temp.nextOperation;
				}
				while(temp != null);
			}
		}
	}

	public void checkVariableLocks(int variableName)
	{
		dlm.checkVariableLocks(variableName);
	}

	public void commit(String transactionName)
	{
		dlm.commit(transactionName);
	}

	public void removeTransaction(Transaction transactionName)//different from the one in DLM
	{
		//check message buffer
		if(messageBuffer.isEmpty() == false)
		{
			for(int i=0; i<messageBuffer.size(); i++)
			{
				operation current = messageBuffer.get(i);
				while(current != null)
				{
					if(current.transactionName.transactionName.equals(transactionName.transactionName))
						current.transactionActive = false;
					current = current.nextOperation;
				}
			}
		}
		deleteVariableChanges(transactionName);
		//if write transaction, this might have written to other variables so erase those copies too
	}

	public boolean checkIfOlder(operation newOperation)
	{
		return dlm.checkIfOlder(newOperation);
	}

	public int noOfLockHolders(int variableName)
	{
		return dlm.noOfLockHolders(variableName);
	}

	public void deleteVariableChanges(Transaction transactionName)//deletes changes made on a variable by a transaction
	{
		dlm.deleteVariableChanges(transactionName);
	}

	public void abortTransaction(operation newOperation)
	{
		dlm.abortTransaction(newOperation);
	}

	public int checkIfReadPossible(operation newOperation)
	{
		return dlm.checkIfReadPossible(newOperation);
	}

	public void failure()//failure caused so clear buffer and forget all lock information
	{
		messageBuffer.clear();
		dlm.forgetLockInformation();
	}

	public void recoverVariable(operation newOperation)
	{
		dlm.recoverVariable(newOperation);
	}

	public String oldestLockHolder(int variableName)
	{
		return dlm.oldestLockHolder(variableName);
	}

	public void printLockInformation()//prints all locked variable information
	{
		dlm.printLockInformation();
	}

	class DataLockManager
	{
		Lock lockInformation[];

		DataLockManager()
		{
			lockInformation = new Lock[21];
			//System.out.println("SITE : " + index);
			for(int i=1; i<=20; i++)
			{
				if(variables[i]!=null)//initialize locks for variables present at that site
					lockInformation[i] = new Lock();
			}
		}

		public void printLockInformation()//prints all locked variable information
		{
			System.out.println("Lock Info for Site " + index);
			for(int i=1; i<=20; i++)
			{
				if(lockInformation[i] != null && isLocked(i) != 'N')
				{
					System.out.print("Variable x" + i + " is locked by Transaction(s) ");
					for(int j=0; j<lockInformation[i].transactionList.size(); j++)//multiple read locks
						System.out.print(lockInformation[i].transactionList.get(j).transactionName + " ");
					System.out.println();
				}
			}
		}

		public char isLocked(int variableName)
		{
			if(lockInformation[variableName].isLocked == false)
				return 'N';//indicates not locked
			else
				return lockInformation[variableName].lockType;
		}

		public void lockVariable(operation newOperation)
		{
			if(newOperation.operationType == 'R')
				newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " reads x" + newOperation.variableName + " = " + variables[newOperation.variableName].value);
			else
				newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " updates temporary value of x" + newOperation.variableName + " to " + newOperation.value + " on Site " + index);
			lockInformation[newOperation.variableName].lockVariable(newOperation);
		}

		public void checkVariableLocks(int variableName)
		{
			lockInformation[variableName].checkLocks(variableName);
		}

		public String oldestLockHolder(int variableName)
		{
			if(lockInformation[variableName].isLocked)
				return lockInformation[variableName].transactionList.get(0).transactionName;
			else
				return null;
		}

		public int noOfLockHolders(int variableName)
		{
			return lockInformation[variableName].transactionList.size();
		}

		public void checkBuffer()
		{
			if(messageBuffer.isEmpty() == false)
			{
				operation current = messageBuffer.get(0);//might be a chain of operations
				processOperation(current);
				messageBuffer.remove(0);
			}
		}

		public void processOperation(operation current)//checks to see if operation can be executed or not
		{
			while(current != null)
			{
				if(current.transactionActive)//check if current transaction hasnt been aborted
				{
					if(current.transactionName.readOnly)
						multiVersionRead(current);
					else
					{
						if(isLocked(current.variableName) != 'W')//old read/empty
						{
							if(current.operationType == 'R')// old read/empty and new read
								lockVariable(current);
						}
						else//already present write lock
						{
							if(current.operationType == 'R')//incoming read operation
							{
								abortTransaction(current);//abort the lockholder
								lockVariable(current);
							}
						}
					}
					current = current.nextOperation;
				}
			}
		}

		public void multiVersionRead(operation newOperation)
		{
			Variable temp = variables[newOperation.variableName];//travel through linked list of variable versions
			do
			{
				if(temp.commitTime > newOperation.transactionName.startTime)
					temp = temp.prevVar;
				else
				{
					newOperation.transactionName.transSummary.add("Transaction " + newOperation.transactionName.transactionName + " reads variable x" + newOperation.variableName + " as " + temp.value);
					break;
				}
			}
			while(temp != null);
		}

		public void abortTransaction(operation newOperation)//abort lock holding transaction
		{
			if(lockInformation[newOperation.variableName].lockType != 'N')
			{
				//System.out.println("LOCKED variable  x" + newOperation.variableName +" on site " + index);
				if(lockInformation[newOperation.variableName].lockType == 'R')//one or more transactions reading, all of which need to be aborted
				{
					lockInformation[newOperation.variableName].isLocked = false;
					lockInformation[newOperation.variableName].lockType = 'N';
					while(lockInformation[newOperation.variableName].transactionList.isEmpty() == false)
					{
						if(!lockInformation[newOperation.variableName].transactionList.get(0).transactionName.equals(newOperation.transactionName.transactionName))//the same transaction did not have a read lock already present on that variable
						{
							//System.out.println("Transaction " + lockInformation[newOperation.variableName].transactionList.get(0).transactionName + " is aborted");
							TransactionManager.removeTransaction(lockInformation[newOperation.variableName].transactionList.get(0));//tell transaction manager to erase changes on all copies of variable
						}
						lockInformation[newOperation.variableName].transactionList.remove(0);
					}
				}
				else//only single transaction is writing on variable so delete changes made by it
				{
					//System.out.println("Transaction " + lockInformation[newOperation.variableName].transactionList.get(0).transactionName + " is aborted");
					TransactionManager.removeTransaction(lockInformation[newOperation.variableName].transactionList.get(0));//tell transaction manager to erase changes on all copies of variable
					lockInformation[newOperation.variableName].isLocked = false;
					lockInformation[newOperation.variableName].lockType = 'N';
					lockInformation[newOperation.variableName].tempValue = -9999;//erase any changes made
				}
			}
		}

		public boolean checkIfOlder(operation newOperation)//check if transaction holding lock for variable is younger than new transaction, or if variable is free
		{
			if(isLocked(newOperation.variableName) != 'N')//variable is locked
			{
				if(newOperation.transactionName.startTime <= lockInformation[newOperation.variableName].transactionList.get(0).startTime)//asking transaction is older than current lock holder (oldest transaction on top of list)
					return true;//since asking is older, put it in waitQueue
				else//asking transaction is younger so abort it
					return false;
			}
			else
				return true;//write can happen on the site's copy or desired variable since it is unlocked
		}

		public void deleteVariableChanges(Transaction transactionName)//remove changes or lock information on any variable made by the transaction to be aborted
		{
			for(int i=1; i<=20; i++)
			{
				if(lockInformation[i] != null && lockInformation[i].isLocked == true)//some type of lock present on variable
				{
					for(int j=0; j<lockInformation[i].transactionList.size(); j++)//check single transaction or list of transactions in case of read lock
					{
						if(lockInformation[i].transactionList.get(j).transactionName.equals(transactionName.transactionName))//transaction present in the list
						{
							//System.out.println("Transaction to be aborted " + lockInformation[i].transactionList.get(j).transactionName);
							lockInformation[i].transactionList.remove(j);
							if(lockInformation[i].lockType == 'W')//if it was a write lock, then change the tempValue also
								lockInformation[i].tempValue = -9999;
							break;//since transaction can be present only once in the list, no need to check the rest
						}
					}
					if(lockInformation[i].transactionList.isEmpty())//if no more transactions in the list, variable is no longer locked
					{
						lockInformation[i].isLocked = false;
						lockInformation[i].lockType = 'N';
					}
				}
			}
		}

		public void commit(String transactionName)//add a new node to the variables array if transaction holding lock commits
		{
			for(int i=1; i<=20; i++)
			{
				if(lockInformation[i] != null)
				{
					if(lockInformation[i].isLocked == true)
					{
						if(lockInformation[i].lockType == 'W')//write lock max one transaction present so reset lock info after adding new node
						{
							if(lockInformation[i].transactionList.get(0).transactionName.equals(transactionName))//write locked by that transaction
							{
							//	Variable nextVar = new Variable(i, lockInformation[i].tempValue, TransactionManager.getTransaction(transactionName).endTime);

Variable nextVar = new Variable(i, lockInformation[i].tempValue, TransactionManager.count);
								System.out.println("Value of variable x" + i + " has been changed from " + variables[i].value + " to " + nextVar.value + " on Site " + index);
								nextVar.prevVar = variables[i];//attach new commited value of variable to linked list
								variables[i] = nextVar;
								lockInformation[i] = new Lock();
								variables[i].validate();//if was in recovery mode, make the variable available for reads
							}
						}
						else//read lock present so remove read lock of commiting transaction from transactionList and reset lock if only transaction
						{
							if(lockInformation[i].transactionList.size() > 1)
							{
								for(int j=1; j<lockInformation[i].transactionList.size(); j++)
								{
									if(lockInformation[i].transactionList.get(j).transactionName.equals(transactionName))//find transaction present in list
									{
										lockInformation[i].transactionList.remove(j);
										break;
									}
								}
							}
							else if(lockInformation[i].transactionList.get(0).transactionName.equals(transactionName))//only one transaction so if read lock present by commiting transaction then reset lock
								lockInformation[i] = new Lock();
						}
					}
				}
			}
		}

		public int checkIfReadPossible(operation newOperation)
		//checks if conflicting lock present
		//return 0 if read possible  with present read lock so send request to that site
		//return 1 if older transaction but write lock present so add to wait queue
		//return 2 if asking is younger so abort it
		//return 3 if same transaction reads it again after writing
		{
			if(lockInformation[newOperation.variableName].lockType != 'W')//irrespective of younger or older transaction
				return 0;
			//if write lock present on variable, check to see if asking transaction older, or same transaction reading
			else
			{
				if(lockInformation[newOperation.variableName].transactionList.get(0).transactionName.equals(newOperation.transactionName.transactionName))//same transaction reading again
					return 3;
				else//different transactions
				{
					if(checkIfOlder(newOperation))//asking is older but write lock present
						return 1;
					else
						return 2;//asking is younger and write lock
				}
			}
		}

		public void forgetLockInformation()
		{
			for(int i=1; i<=20; i++)
			{
				if(lockInformation[i] != null)
				{
					lockInformation[i].clearLock();
					if(i % 2 == 0)//only for replicated variables
						variables[i].invalidate();
				}
			}
		}

		public void recoverVariable(operation newOperation)//recovers the variable from other sites
		{
			Variable varToCopyFrom = TransactionManager.askForAvailableSite(newOperation);//ask TM for a variable to copy from an available site
			if(varToCopyFrom == null)//no site to recover from as all are down
			{
				//System.out.println("No site to recover from so end program");
				//System.exit(0);
				return;
			}
			Variable varToCopyTo = new Variable(varToCopyFrom.index, varToCopyFrom.value, varToCopyFrom.commitTime);//initialize new linked list
			variables[newOperation.variableName] = varToCopyTo;//make the current sites variable to be recovered point to the latest node of new linked list
			//copy linked list of commited versions of the variable
			varToCopyFrom = varToCopyFrom.prevVar;
		 	while(varToCopyFrom != null)
		 	{
				varToCopyTo.prevVar = new Variable(varToCopyFrom.index, varToCopyFrom.value, varToCopyFrom.commitTime);
				varToCopyFrom = varToCopyFrom.prevVar;//keep going backward till you read the last commited version of variable
				varToCopyTo = varToCopyTo.prevVar;
			}
			variables[newOperation.variableName].invalidate();//variable is still not valid until committed value written upon it
		}
	}
}

