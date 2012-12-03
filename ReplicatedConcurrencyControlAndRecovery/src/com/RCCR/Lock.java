package com.RCCR;
import java.util.ArrayList;


public class Lock {

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
