import java.util.ArrayList;

class TransactionManager
{
	Site sites[];                    // All sites managed by the TM
	ArrayList<Waitobj> tm_queue;     // A wait queue at the TM
	TransactionManager()
	{
		sites=new Site[11];
		for(int i=1;i<11;i++)
		{
			sites[i]=new Site(i);
		}

		tm_queue = new ArrayList<Waitobj>();
	}

	String readOnly(Transaction t, int varno)        // Read operation for ReadOnly transaction t for
	{                                                // variable varno
		int sitefail=0;
		String readOnly="";
		
		for(int i=1;i<sites.length;i++)
		{
			if(sites[i].isUp() && sites[i].present[varno]==true && sites[i].can_read[varno]==true)
			{

				readOnly +=  "Multiversion Read";
				readOnly  += "\n";
				readOnly += t.readOnly(varno, sites[i]);
				readOnly  += "\n";
				break;

			}
			else
			{
				if(!sites[i].isUp() && sites[i].present[varno]==true)
				{
					sitefail++;
				}


				if(varno%2==0 && sitefail==10)
				{
					// All relevant sites have failed for the read - operation of a even indexed variable
					readOnly +=  "All relevant sites failed and the transaction " + t.name +" is queued.";
					readOnly  += "\n";
					Waitobj tw3=new Waitobj(t,varno,'R',0);
					tm_queue.add(tw3);
					return readOnly;

				}
				else if(varno%2==1 && sitefail==1)
				{
					// The site holding the non replicated variable is down
					readOnly +=  "The relevant site failed and the transaction " + t.name +" is queued.";
					readOnly  += "\n";
					Waitobj tw2=new Waitobj(t,varno,'R',0);
					tm_queue.add(tw2);
					return readOnly;

				}
			}

		}
		return readOnly;
	}

	String read(Transaction t, int varno)      // Read operation for transaction t on variable varno
	{
		boolean flag=false;
		int age_oldest=32767,temp=32767;
		int sitefail=0;
		String readString="";

		if(t.type.equals("ReadOnly"))
		{
			readString  +=  readOnly(t,varno);
			readString  += "\n";
			return readString;
		}
		outer: for(int i=1;i<sites.length;i++)
		{
			for(int j=0;j<sites[i].lockTrans[varno].size();j++)
			{
				if(sites[i].lockTrans[varno].get(j).equals(t) && sites[i].can_read[varno]==true) //Same Transaction read
				{
					flag=true;
					readString  += t.read(varno, sites[i]);
					readString  += "\n";
					break outer;
				}
			}
			if(sites[i].isUp() && sites[i].present[varno]==true && sites[i].can_read[varno]==true)
			{
				flag=sites[i].lock(t, varno, 'R');
				if(flag)
				{
					readString  +=  "Transaction " + t.name + " has locked successfully at site " + i + " for read";
					readString  += "\n";
					readString  += t.read(varno, sites[i]);
					readString  += "\n";
					t.site_access.add(i);
					t.all_access.add(varno);
					break;
				}
			}
			if(!sites[i].isUp() && sites[i].present[varno]==true)
			{
				sitefail++;
			}

		}
		if(varno%2==0 && sitefail==10)
		{
			readString  +=  "All relevant sites failed and the transaction " + t.name + "is queued.";
			readString  += "\n";
			Waitobj tw3=new Waitobj(t,varno,'R',0);
			tm_queue.add(tw3);
			return readString;

		}
		else if(varno%2==1 && sitefail==1)
		{
			readString  +=  "The relevant site failed and the transaction" + t.name+ " is queued.";
			readString  += "\n";
			Waitobj tw2=new Waitobj(t,varno,'R',0);
			tm_queue.add(tw2);
			return readString;

		}
		if(!flag)
		{
			for(int i=1;i<sites.length;i++)
			{
				if(sites[i].isUp() && sites[i].present[varno]==true)
				{
					temp=sites[i].returnoldTS(varno);
					if(temp<age_oldest)
					{
						age_oldest=temp;
					}
				}
			}
			if(age_oldest>t.timestamp)
			{                              // Queue the transaction as its the oldest
				for(int i=1;i<sites.length;i++)
				{
					if(sites[i].isUp() && sites[i].present[varno]==true)
					{
						Waitobj w1=new Waitobj(t,varno,'R',0);
						sites[i].w_queue.add(w1);
						readString  +=  t.name+" Queued at site" + i;
						readString  += "\n";
						break;
					}
				}
			}
			else
			{ // Kill the transaction as it is younger
				readString += "Transaction will be killed " + t.name;
				readString  += "\n";
				t.aborted=true;
				t.abort();
				readString += end(t);
				readString  += "\n";
				t.is_kill=true;
			}
		}
		return readString;
	}


	String write(Transaction t, int varno, int value)    // Write operation for transaction t on
	{                                                    // variable varno with given value
		boolean flag=false,flag_2=false;
		int sitefail1=0;
		Transaction tx;
		String writeString="";
		outer1: for(int i=1;i<sites.length;i++)
		{
			if( !sites[i].isUp()  &&  sites[i].present[varno]==true )
			{
				sitefail1++;
			}

			else if( sites[i].isUp()  &&  sites[i].present[varno]==true )
			{
				if(sites[i].returnLockType(varno)!='E')
				{
					if(sites[i].lockTrans[varno].size()>0)
					{
						for(int k=0;k<sites[i].lockTrans[varno].size();k++)
						{
							tx=(Transaction)sites[i].lockTrans[varno].get(k);
							if(!tx.name.equals(t.name))
							{
								flag_2=false;
								flag=false;
								break outer1;
							}
							else flag_2=true;
						}
					}

				}
				else
					flag=true;
			}
		}

		if( varno % 2 ==0  &&  sitefail1==10 )
		{
			writeString += "All relevant sites have failed and the transaction " + t.name+" is queued.";
			writeString += "\n";
			Waitobj tw=new Waitobj(t,varno,'W',value);
			tm_queue.add(tw);

			return writeString;
		}
		else if( varno % 2 == 1  &&  sitefail1 == 1 )
		{
			writeString += "The relevant site failed and the transaction" + t.name + "is queued.";
			writeString += "\n";
			Waitobj tw1=new Waitobj(t,varno,'W',value);
			tm_queue.add(tw1);

			return writeString;
		}

		if(flag==false  &&  flag_2==false)
		{
			int age_old=32767,temp=32767;
			for(int i=1;i<sites.length;i++)
			{
				if(sites[i].isUp() && sites[i].present[varno]==true)
				{
					temp=sites[i].returnoldTS(varno);
					if(temp<age_old)
						age_old=temp;
				}
			}
			if(age_old>=t.timestamp)
			{    // Queue the transaction as its the oldest
				Waitobj w3;
			for(int i=1;i<sites.length;i++)
			{
				if(sites[i].isUp() && sites[i].present[varno]==true)
				{
					if(sites[i].returnLockType(varno)!='E')
					{
						w3=new Waitobj(t,varno,'W',value);
						sites[i].w_queue.add(w3);
						writeString += t.name+" Queued at site" + i;
						writeString += "\n";
						break;
					}
				}
			}
			}
			else
			{// Kill the transaction as its younger
				writeString += "Transaction will be killed " + t.name;
				writeString += "\n";
				t.abort();
				t.aborted=true;
				writeString += end(t);
				writeString += "\n";
				t.is_kill=true;

				return writeString;
			}
		}
		else if(flag==false  &&  flag_2==true) //Same Transaction writes again
		{

			for(int i=1;i<sites.length;i++)
			{
				if(sites[i].isUp()  &&  sites[i].present[varno]==true)
				{
					sites[i].locktype[varno]='W';
					sites[i].can_read[varno]=true;
					writeString += t.write(varno, sites[i], value);
					writeString += "\n";
				}
			}
			t.var_access.add(varno);
			t.all_access.add(varno);
		}

		else
		{
			for(int i=1;i<sites.length;i++)
			{
				if(sites[i].isUp() && sites[i].present[varno]==true)
				{
					sites[i].lock(t, varno, 'W');
					sites[i].locktype[varno]='W';
					sites[i].can_read[varno]=true;
					writeString += t.write(varno, sites[i], value);
					writeString += "\n";
				}
			}
			t.var_access.add(varno);
			t.all_access.add(varno);
		}
		return writeString;
	}

	//Querystate function to know the status of the data of the data and lock managers
	String querystate()
	{
		String queryStateString="";

		queryStateString  +=  "QueryState";
		queryStateString  +=  "\n";

		for(int i=1;i<11;i++)
		{
			for(int j=1;j<21;j++)
			{
				for(int k=0;k<sites[i].lockTrans[j].size();k++)
				{
					if(sites[i].locktype[j] != 'E'   &&  sites[i].present[j] == true)
					{
						Transaction x=(Transaction)(sites[i].lockTrans[j].get(k));
						queryStateString  +=  "Variable x" + j + " is locked with  " + sites[i].locktype[j] + " at site " + i + " by " + x.name;
						queryStateString  +=  "\n";
					}
				}
			}
		}

		return queryStateString;
	}

	String dump()      // Display the committed values of all variables at all sites
	{
		String dumpString="";
		dumpString  +=  "Dumping Everything";
		dumpString  +=  "\n";

		for(int i=1;i<sites.length;i++)
		{
			if(sites[i].isUp())
			{
				for (int j=1;j<sites[i].present.length;j++)
				{
					if( sites[i].present[j]== true )
					{

						dumpString  +=  "Variable at site" + i;
						dumpString  +=  "\n";
						dumpString  +=  sites[i].variable[j].returnName() + "=" +sites[i].variable[j].returnValue();
						dumpString  +=  "\n";

					}
				}
			}

		}
		return dumpString;
	}

	String dump(Site s)    // Display the committed values of all variables at site s
	{
		String dumpSiteString="";
		dumpSiteString  +=  "Dumping At site:" + s.sitenumber;
		dumpSiteString  +=  "\n";
		for(int i=1;i<sites.length;i++)
		{
			if(sites[i].sitenumber == s.sitenumber)
			{
				if(sites[i].isUp())
				{
					for (int j=1;j<sites[i].present.length;j++)
					{
						if( sites[i].present[j]== true)
						{

							dumpSiteString  +=  "Variable at site" + i;
							dumpSiteString  +=  "\n";
							dumpSiteString  +=  sites[i].variable[j].returnName() + "=" +sites[i].variable[j].returnValue();
							dumpSiteString  +=  "\n";

						}
					}
				}
			}

		}
		return dumpSiteString;
	}



	String dump(Variable v)          // Display the committed values of variable v at all sites
	{
		String dumpVariableString="";
		dumpVariableString  +=  "Dumping the variable: "+ v.returnNo();
		dumpVariableString  +=  "\n";

		for(int i=1;i<sites.length;i++)
		{
			if(sites[i].isUp())
			{
				if( sites[i].present[v.returnNo()]== true)
				{

					dumpVariableString  +=  "Variable at site" + i;
					dumpVariableString  +=  "\n";
					dumpVariableString  +=  sites[i].variable[v.returnNo()].returnName() + "=" +sites[i].variable[v.returnNo()].returnValue();
					dumpVariableString  +=  "\n";

				}
			}

		}
		return dumpVariableString;

	}


	void releaseLocks(Transaction t)               // Release all locks held by transaction t
	{
		for(int i=1;i<sites.length;i++)
		{
			sites[i].unlock(t);
		}
	}

	boolean checkforcommit(Transaction t)          // Check if transaction t can be committed
	{
		int s;
		boolean flag=false;

		for(int i=0;i<t.site_access.size();i++)
		{
			s=t.site_access.get(i);
			if(sites[s].isUp())
			{
				flag=true;
			}
			else
			{
				flag=false;
				break;
			}
		}

		return flag;
	}

	String callFunction(Waitobj wo)                // Execute waiting operations
	{
		Transaction tx = wo.t;
		int v= wo.var;
		char oper= wo.op;
		int valx= wo.value1;
		String buffer="";
		switch(oper)
		{
		case 'R' :
			buffer += "Executing the read now for " + tx.name;
			buffer += "\n";
			buffer += read(tx,v);
			buffer += "\n";
			break;
		case 'W' :
			buffer += "Executing the write now for " + tx.name;
			buffer += "\n";
			buffer += write(tx,v,valx);
			buffer += "\n";
		}
		return buffer;
	}


	String end(Transaction t)                   // End the transaction by either committing or aborting
	{
		String endString="";
		if(t.is_kill==true)
		{
			endString  +=  "Transaction has been killed previously " + t.name ;
			endString  +=  "\n";
			return endString;
		}
		boolean commitflag = false;
		boolean decideflag = false;
		if (t.type.equalsIgnoreCase("ReadOnly")== true)
		{ 
			commitflag =true;
			endString += "Read only Transaction ends "+ t.name;
			endString  +=  "\n";
		}
		else
		{
			decideflag =checkforcommit(t);
			if(t.aborted==true)
				decideflag=false;
			endString  +=  "Decision to commit is " + decideflag + " for transaction "+t.name;
			endString  +=  "\n";
			if(decideflag == true)
			{
				int y;
				if(t.has_write)
				{
					for(int j=0;j<t.var_access.size();j++)
					{
						y=t.var_access.get(j);
						for(int i=1;i<sites.length;i++)
						{
							if(sites[i].present[y] == true)
							{
								endString += t.writeAtCommit(sites[i]);
								endString += "\n";
								sites[i].written[y]=false;
							}
						}
					}
				}
				releaseLocks(t);
				commitflag=true;
			}
			else
			{
				int x;

				if(t.has_write)
				{
					for(int j=0;j<t.var_access.size();j++)
					{
						x=t.var_access.get(j);

						for(int i=1;i<sites.length;i++)
						{
							sites[i].written[x]=false;
						}
					}

				}
				t.abort();
				releaseLocks(t);
				commitflag=false;
			}
		}
		Waitobj w3;
		for(int i=1;i<sites.length;i++)
		{

			for(int k=0;k<sites[i].w_queue.size();k++)
			{

				w3=(Waitobj)(sites[i].w_queue.get(k));
				endString  +=  "Found waiting transaction at site "+i;
				endString  +=  "\n";
				if(t.all_access.contains(w3.var))
				{
					endString  +=  "The variable is " + w3.var;                       
					endString  +=  "\n";
					endString += callFunction(w3);
					sites[i].w_queue.remove(k);
					endString += "\n";
				}
			}

		}


		return endString;
	}

	void waitrecovery(int siteno)        // Execute waiting operations in the queue of the TM
	{
		Waitobj wx;
		for(int i=0;i<tm_queue.size();i++)
		{
			wx=tm_queue.get(i);
			if(sites[siteno].present[wx.var] == true)
			{
				callFunction(wx);
				tm_queue.remove(i);
			}
		}
	}
}
