
import java.io.*;
import java.util.*;
// Class for maintaining snapshots of variables for read only transaction
class copy
 {
     int val;
     int ts;
     copy(){}
     copy(int v,int t)
     {
         val=v;
         ts=t;
     }
     int returnTS()
     {
         return ts;
     }
     int returnVal()
     {
         return val;
     }
 }

//Class to maintain each variable

 class Variable
 {
           String name;
          int value;
          Vector variableCopies;

           Variable(int value,String name1)
          {
              this.value = value;
              this.name = name1;
              variableCopies=new Vector();
          }
           Variable(String name1)
           {

               this.name = name1;
               variableCopies=new Vector();
           }

          void write(int value, int ts)                // write a new value to the variable
          {
               this.value = value;
               copy x=new copy(value,ts);
               variableCopies.add(x);
          }

          int returnValue()                            // return the variable value
          {
              return this.value;
          }

          String returnName()                           // return the name of the variable
          {
              return this.name;
          }


          int returnNo()                               // returns the no of the variable i.e. 3 for x3
          {
              if(name.length()==2)
              {
                  return (int)(name.charAt(1) - 48);
              }
              else
              {
                  return Integer.parseInt(name.substring(1));
              }
          }
          boolean isReplicated()                          // checks if variable is replicated i.e. true for even variables
          {
              int x;
               if (name.length() == 2)
               {
                   x=(int)(name.charAt(1)-48);
                   System.out.println(x);
               }
               else
               {
                   String d=name.substring(1);
                   x=Integer.parseInt(d);
                   System.out.println(x);
               }
               if (x % 2 == 0)
               {
                   return true;
               }
               else
               {
                   return false;
               }

          }
          int read(String type,int ts1)                       // read the value of the variable
          {

              return value;

          }

          int readOnly(String type,int ts1)                   // read an appropriate snapshot of the variable
          {
                  copy x=new copy();
                  for(int i=0;i<variableCopies.size();i++)
                  {
                      x=(copy)variableCopies.elementAt(i);
                      if(x.ts<ts1)
                      {
                          continue;
                      }
                      else
                      {
                          copy y=(copy)variableCopies.elementAt(i-1);
                          return y.val;
                      }
                  }


              return x.val;

          }

          void printVector()
          {
              copy c;
              System.out.println("Printing the Vector for variable "+this.name);
              for(int i=0;i<variableCopies.size();i++)
              {
                  c=(copy)(variableCopies.get(i));
                  System.out.println("Value "+c.val+" Timestamp"+ c.ts);
              }
          }
 }

    // class for describing the sites
     class Site
     {
         int sitenumber;
         Variable variable[];                           // contains all the variables present at the site
         boolean present[];                             // indicates the variables present at the site
         char locktype[];                               // indicates the lock at the variable
         Vector lockTrans[];                            // indicates the transaction which holds the lock
         ArrayList <Waitobj>w_queue;                    // Stores blocked transactions
         boolean fail=false;                            // indicates if the site has failed
         boolean can_read[];                            // indicates if a read is allowed on the variable
         Boolean []written={false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,
                 false,false,false,false,false};
         int []writeValues={0,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200};


         Site(int sitenumber)
         {
             String name2="x";
             this.sitenumber=sitenumber;
             variable=new Variable[21];
             present=new boolean[21];
             locktype=new char[21];
             can_read=new boolean[21];
             for(int i=1;i<can_read.length;i++)
                 can_read[i]=true;
             for(int i=1;i<locktype.length;i++)
                 locktype[i]='E';
             lockTrans=new Vector[21];
             for(int i=1;i<21;i++)
                 lockTrans[i]=new Vector();

            w_queue=new ArrayList<Waitobj>();
             for(int i=1;i<variable.length;i++)
             {
                 name2="x";
                 name2=name2+(i);
                 variable[i]=new Variable(name2);
             }
             for(int i=1;i<present.length;i++)
             {

                 if(i%2==0||(((i%10)+1)==sitenumber))
                 {
                     present[i]=true;
                     variable[i].write(10*i,0);
                 }
                 else
                     present[i]=false;

             }


         }
         char returnLockType(int varno)        // returns the type of lock on the variable varno
         {
               return locktype[varno];
         }
         void displayT(int v)
         {
             if(lockTrans[v].size()==0)
             {
                 System.out.println("Empty");
             }
             else
             {
                 for(int i=0;i<lockTrans[v].size();i++)
                 {
                     Transaction x=(Transaction)(lockTrans[v].get(i));
                 }
             }
         }
         boolean lock(Transaction t,int varno,char type)            // tries to lock the variable varno for
         {                                                          // transaction t with requested lock type

             char c=returnLockType(varno);

             if((c=='R'&&type=='R')||(c=='E'))
             {

                 lockTrans[varno].add(t);
                 locktype[varno]=type;

                 for(int i=0;i<lockTrans[varno].size();i++)
                 {
                     Transaction y=(Transaction)lockTrans[varno].get(i);
                 }
                 return true;
             }
             else
             {
                 return false;
             }
         }

         void unlock(Transaction x)                      // releases all locks held by transaction x
         {
             Transaction t;
             for(int i=1;i<lockTrans.length;i++)
             {
                 for(int j=0;j<lockTrans[i].size();j++)
                 {
                     t=(Transaction)(lockTrans[i].get(j));
                     if(t.name==x.name)
                     {

                         lockTrans[i].remove(j);
                         if(lockTrans[i].size()==0)
                             locktype[i]='E';
                     }
                 }
             }
         }

         int returnoldTS(int varno)                     // returns the timestamp of the oldest transaction
         {                                              // holding a lock on variable varno
             int old=32767;
             for(int i=0;i<lockTrans[varno].size();i++)
             {
                 Transaction t=(Transaction)lockTrans[varno].get(i);

                 if(old > t.timestamp)
                 {
                     old=t.timestamp;

                 }
             }
             Waitobj w;
             int temp;
             for(int i=0;i<w_queue.size();i++)
             {
                 w=(Waitobj)(w_queue.get(i));
                 temp=w.t.timestamp;
                 if(temp<old&&w.var==varno)
                 {
                     old=temp;;
                 }
             }
             return old;
         }


         boolean isUp()                       // indicates if the site is up
         {
             return !fail;
         }

         void recover()                        // causes the site to recover
         {
             fail=false;
         }

         Waitobj[] fail()                        // causes the site to fail
         {
        	 Waitobj wt[]=new Waitobj[20];
             for(int i=1;i<can_read.length;i++)
             {
                 if(i%2==0||present[i]==false)
                  can_read[i]=false;
             }
             fail = true;
             for(int i=1;i<variable.length;i++)
             {
                 locktype[i]='E';
                 lockTrans[i].removeAllElements();
             }
             for(int i=1;i<written.length;i++)
             {
            	 written[i] = false;
             }
             Waitobj ww;
             int count=0;
             for(int j=0;j<w_queue.size();j++)
             {
                 ww=w_queue.get(j);
              	 wt[count]=new Waitobj(ww.t,ww.var,ww.op,ww.value1);
              	 count++;
                 w_queue.remove(j);
             }
             return wt;
         }
     }

 // Class for describing a transaction

 class Transaction
 {
     String name;                  // name of the transaction
     int timestamp;                // timestamp of the transaction
     String type;                  // Indicates if transaction is of type Read-Write or Readonly
     boolean aborted=false;        // indicates if the transaction is to be aborted
     boolean has_write=false;      // indicates if the transaction has performed a write op
     boolean is_kill=false;        // indicates if the transaction is to be killed
     Vector<Integer> site_access;  // stores no of sites accessed by the Transaction
     Vector<Integer> var_access;   // stores the no of all variables written to by the transaction
     Vector<Integer> all_access;
     Transaction()
     {

     }

     Transaction(String n,int tp,String t)
     {
         name=n;
         timestamp=tp;
         type=t;
         site_access=new Vector<Integer>();
         var_access=new Vector<Integer>();
         all_access=new Vector<Integer>();
     }





     String read(int varno , Site s)                 // Read operation for the transaction for variable
     {                                               // varno at site s

    	 String readInside="";
         if(s.written[varno]==false)
         {
         int val=s.variable[varno].read(this.type,this.timestamp);
         readInside+=val+" accessed for read at site "+s.sitenumber;
         readInside+="\n";
         }
         else
         {
             int val=s.writeValues[varno];
             readInside+=val+" accessed for read at site "+s.sitenumber + "by Transaction " + name;
             readInside+="\n";
         }
         return readInside;
     }

     String readOnly(int varno,Site s)              // Read operation for Readonly transaction for
     {                                              // variable varno at site s
    	 String rOnly="";
         int val=s.variable[varno].readOnly(this.type,this.timestamp);
         rOnly+= val+" accessed read only at site "+s.sitenumber + "by Transaction " + name;
         rOnly+="\n";
         return rOnly;
     }

     String writeAtCommit(Site s)                    // Commits all writes performed by the transaction
     {                                               // at site s
    	 String commitWriteString="";
    	 commitWriteString += "Doing final write for "+name;
    	 commitWriteString += "\n";

         for(int i=1;i<s.written.length;i++)
         {

             if (s.written[i]== true&&var_access.contains(i)==true)
             {
                 s.variable[i].write(s.writeValues[i],this.timestamp);
                 commitWriteString += "Committed write at site" +s.sitenumber;
            	 commitWriteString += "\n";

             }
         }
         return commitWriteString;

     }
     String write(int varno, Site s,int value)   // write operation for transaction for variable varno
     {                                         // at site s
         String uncommWrite="";
         uncommWrite+= "Transaction " + name + " Performing uncommitted write for variable " + varno;
         uncommWrite+= "\n";
         s.writeValues[varno]=value;
         s.written[varno]=true;
         has_write=true;
         site_access.add(s.sitenumber);
         return uncommWrite;
       }

     String abort()                            // Just an indication that the transaction has been aborted
     {
    	 String abortString="";
         abortString+="Aborted" + name;
         abortString+= "\n";
         return abortString;

     }
 }

 // Class for describing waiting transactions on specific operations
 class Waitobj
 {
     Transaction t;
     int var;
     char op;
     int value1;
     Waitobj(Transaction x, int v, char o, int p)
     {
         t=x;
         var=v;
         op=o;
         value1=p;
     }
     Waitobj()
     {
         System.out.println("Please initiate properly");
     }
     void display()
     {
         System.out.println("The waiting transaction is:"+t.name);
         System.out.println("on operation "+op+" on variable x"+var);

     }
 }

 // Class describing the structure of the Transaction Manager
 class TransactionManager
 {
     Site site1[];                    // All sites managed by the TM
     ArrayList<Waitobj> tm_queue;     // A wait queue at the TM
     TransactionManager()
     {
         site1=new Site[11];
         for(int i=1;i<11;i++)
         {
             site1[i]=new Site(i);
         }

         tm_queue = new ArrayList<Waitobj>();
     }

     String readOnly(Transaction t, int varno)        // Read operation for ReadOnly transaction t for
     {                                                // variable varno
    	 int sitefail=0;
    	 String readOnly="";
         for(int i=1;i<site1.length;i++)
         {
             if(site1[i].isUp()&&site1[i].present[varno]==true&&site1[i].can_read[varno]==true)
             {

                     System.out.println("multiversion read");
                     readOnly+= "Multiversion Read";
                     readOnly +="\n";
                     readOnly+=t.readOnly(varno, site1[i]);
                     readOnly +="\n";
                     break;

             }
             else
             {
             if(!site1[i].isUp()&&site1[i].present[varno]==true)
             {
                  sitefail++;
             }


         if(varno%2==0&&sitefail==10)
         {
            // All relevant sites have failed for the read - operation of a even indexed variable
            readOnly+= "All relevant sites failed and the transaction " + t.name +" is queued.";
            readOnly +="\n";
            Waitobj tw3=new Waitobj(t,varno,'R',0);
            tm_queue.add(tw3);
            return readOnly;

         }
         else if(varno%2==1&&sitefail==1)
         {
        	// The site holding the non replicated variable is down
            readOnly+= "The relevant site failed and the transaction " + t.name +" is queued.";
            readOnly +="\n";
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
        	 readString += readOnly(t,varno);
             readString +="\n";
             return readString;
         }
         outer: for(int i=1;i<site1.length;i++)
         {
             for(int j=0;j<site1[i].lockTrans[varno].size();j++)
             {
             if(site1[i].lockTrans[varno].get(j).equals(t)&&site1[i].can_read[varno]==true) //Same Transaction read
             {
                 flag=true;
                 readString +=t.read(varno, site1[i]);
                 readString +="\n";
                 break outer;
             }
             }
             if(site1[i].isUp()&&site1[i].present[varno]==true&&site1[i].can_read[varno]==true)
             {
                 flag=site1[i].lock(t, varno, 'R');
                 if(flag)
                 {
                     readString += "Transaction " + t.name + " has locked successfully at site " + i + " for read";
                     readString +="\n";
                     readString +=t.read(varno, site1[i]);
                     readString +="\n";
                     t.site_access.add(i);
                     t.all_access.add(varno);
                     break;
                 }
             }
             if(!site1[i].isUp()&&site1[i].present[varno]==true)
             {
                  sitefail++;
             }

         }
         if(varno%2==0&&sitefail==10)
         {
             readString += "All relevant sites failed and the transaction " + t.name + "is queued.";
             readString +="\n";
             Waitobj tw3=new Waitobj(t,varno,'R',0);
             tm_queue.add(tw3);
             return readString;

         }
         else if(varno%2==1&&sitefail==1)
         {
             readString += "The relevant site failed and the transaction" + t.name+ " is queued.";
             readString +="\n";
             Waitobj tw2=new Waitobj(t,varno,'R',0);
             tm_queue.add(tw2);
             return readString;

         }
         if(!flag)
         {
             for(int i=1;i<site1.length;i++)
             {
                 if(site1[i].isUp()&&site1[i].present[varno]==true)
                 {
                     temp=site1[i].returnoldTS(varno);
                     if(temp<age_oldest)
                     {
                         age_oldest=temp;
                     }
                 }
             }
             if(age_oldest>t.timestamp)
             {                              // Queue the transaction as its the oldest
                 for(int i=1;i<site1.length;i++)
                 {
                     if(site1[i].isUp()&&site1[i].present[varno]==true)
                     {
                    	 Waitobj w1=new Waitobj(t,varno,'R',0);
                         site1[i].w_queue.add(w1);
                         readString += t.name+" Queued at site" + i;
                         readString +="\n";
                         break;
                     }
                 }
             }
             else
             { // Kill the transaction as it is younger
                 readString+="Transaction will be killed " + t.name;
                 readString +="\n";
                 t.aborted=true;
                 t.abort();
                 readString+=end(t);
                 readString +="\n";
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
        outer1: for(int i=1;i<site1.length;i++)
         {
        	 if(!site1[i].isUp()&&site1[i].present[varno]==true)
             {
                  sitefail1++;
             }

        	 else if(site1[i].isUp()&&site1[i].present[varno]==true)
             {
                if(site1[i].returnLockType(varno)!='E')
                {
                    if(site1[i].lockTrans[varno].size()>0)
                    {
                        for(int k=0;k<site1[i].lockTrans[varno].size();k++)
                        {
                            tx=(Transaction)site1[i].lockTrans[varno].get(k);
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

         if(varno%2==0&&sitefail1==10)
         {
             writeString+="All relevant sites have failed and the transaction " + t.name+" is queued.";
             writeString+="\n";
             Waitobj tw=new Waitobj(t,varno,'W',value);
             tm_queue.add(tw);

             return writeString;
         }
         else if(varno%2==1&&sitefail1==1)
         {
             writeString+="The relevant site failed and the transaction" + t.name + "is queued.";
             writeString+="\n";
             Waitobj tw1=new Waitobj(t,varno,'W',value);
             tm_queue.add(tw1);

             return writeString;
         }

         if(flag==false && flag_2==false)
         {
             int age_old=32767,temp=32767;
             for(int i=1;i<site1.length;i++)
             {
                 if(site1[i].isUp()&&site1[i].present[varno]==true)
                 {
                     temp=site1[i].returnoldTS(varno);
                     if(temp<age_old)
                         age_old=temp;
                 }
             }
             if(age_old>=t.timestamp)
             {    // Queue the transaction as its the oldest
                 Waitobj w3;
                 for(int i=1;i<site1.length;i++)
                 {
                     if(site1[i].isUp()&&site1[i].present[varno]==true)
                     {
                         if(site1[i].returnLockType(varno)!='E')
                         {
                             w3=new Waitobj(t,varno,'W',value);
                             site1[i].w_queue.add(w3);
                             writeString+=t.name+" Queued at site" + i;
                             writeString+="\n";
                             break;
                         }
                     }
                 }
             }
             else
             {// Kill the transaction as its younger
                 writeString+="Transaction will be killed " + t.name;
                 writeString+="\n";
                 t.abort();
                 t.aborted=true;
                 writeString+=end(t);
                 writeString+="\n";
                 t.is_kill=true;

                 return writeString;
             }
         }
         else if(flag==false && flag_2==true) //Same Transaction writes again
         {

             for(int i=1;i<site1.length;i++)
             {
                if(site1[i].isUp() && site1[i].present[varno]==true)
                {
                    site1[i].locktype[varno]='W';
                    site1[i].can_read[varno]=true;
                    writeString+=t.write(varno, site1[i], value);
                    writeString+="\n";
                }
             }
             t.var_access.add(varno);
             t.all_access.add(varno);
         }

         else
         {
             for(int i=1;i<site1.length;i++)
             {
                 if(site1[i].isUp()&&site1[i].present[varno]==true)
                 {
                     site1[i].lock(t, varno, 'W');
                     site1[i].locktype[varno]='W';
                     site1[i].can_read[varno]=true;
                     writeString+=t.write(varno, site1[i], value);
                     writeString+="\n";
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

    	 queryStateString += "QueryState";
    	 queryStateString += "\n";

    	 for(int i=1;i<11;i++)
	     {
	    	 for(int j=1;j<21;j++)
	    	 {
	    		 for(int k=0;k<site1[i].lockTrans[j].size();k++)
	    		 {
    	 if(site1[i].locktype[j] != 'E'  && site1[i].present[j] == true)
    	 {
    		Transaction x=(Transaction)(site1[i].lockTrans[j].get(k));
    		queryStateString += "Variable x" + j + " is locked with  " + site1[i].locktype[j] + " at site " + i + " by " + x.name;
    		queryStateString += "\n";
    	 }
	     }
	     }
	     }

    	  return queryStateString;
     }

     String dump()      // Display the committed values of all variables at all sites
     {
    	 String dumpString="";
    	 dumpString += "Dumping Everything";
    	 dumpString += "\n";

         for(int i=1;i<site1.length;i++)
         {
             if(site1[i].isUp())
             {
                 for (int j=1;j<site1[i].present.length;j++)
                 {
                 if( site1[i].present[j]== true )
                 {

                dumpString += "Variable at site" + i;
                dumpString += "\n";
                dumpString += site1[i].variable[j].returnName() + "=" +site1[i].variable[j].returnValue();
                dumpString += "\n";

                 }
                 }
             }

         }
         return dumpString;
     }

     String dump(Site s)    // Display the committed values of all variables at site s
     {
    	 String dumpSiteString="";
    	 dumpSiteString += "Dumping At site:" + s.sitenumber;
    	 dumpSiteString += "\n";
         for(int i=1;i<site1.length;i++)
         {
             if(site1[i].sitenumber == s.sitenumber)
             {
             if(site1[i].isUp())
             {
                 for (int j=1;j<site1[i].present.length;j++)
                 {
                 if( site1[i].present[j]== true)
                 {

                  dumpSiteString += "Variable at site" + i;
                  dumpSiteString += "\n";
                  dumpSiteString += site1[i].variable[j].returnName() + "=" +site1[i].variable[j].returnValue();
                  dumpSiteString += "\n";

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
    	 dumpVariableString += "Dumping the variable: "+ v.returnNo();
    	 dumpVariableString += "\n";

         for(int i=1;i<site1.length;i++)
         {
             if(site1[i].isUp())
             {
                 if( site1[i].present[v.returnNo()]== true)
                 {

                dumpVariableString += "Variable at site" + i;
                dumpVariableString += "\n";
                dumpVariableString += site1[i].variable[v.returnNo()].returnName() + "=" +site1[i].variable[v.returnNo()].returnValue();
                dumpVariableString += "\n";

                 }
             }

         }
         return dumpVariableString;

     }


     void releaseLocks(Transaction t)               // Release all locks held by transaction t
     {
         for(int i=1;i<site1.length;i++)
         {
             site1[i].unlock(t);
         }
     }

     boolean checkforcommit(Transaction t)          // Check if transaction t can be committed
     {
         int s;
         boolean flag=false;

         for(int i=0;i<t.site_access.size();i++)
         {
             s=t.site_access.get(i);
             if(site1[s].isUp())
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
                     buffer+="Executing the read now for " + tx.name;
                     buffer+="\n";
                     buffer+=read(tx,v);
                     buffer+="\n";
                     break;
           case 'W' :
        	         buffer+="Executing the write now for " + tx.name;
        	         buffer+="\n";
        	         buffer+=write(tx,v,valx);
                     buffer+="\n";
         }
         return buffer;
     }


     String end(Transaction t)                   // End the transaction by either committing or aborting
     {
    	 String endString="";
    	 if(t.is_kill==true)
    	 {
    		 endString += "Transaction has been killed previously " + t.name ;
    		 endString += "\n";
    		 return endString;
    	 }
         boolean commitflag = false;
         boolean decideflag = false;
         if (t.type.equalsIgnoreCase("ReadOnly")== true)
         {
             commitflag =true;
             endString+="Read only Transaction ends"+ t.name;
             endString += "\n";
         }
         else
         {
             decideflag =checkforcommit(t);
             if(t.aborted==true)
            	 decideflag=false;
             endString += "Decision to commit is " + decideflag + " for transaction "+t.name;
             endString += "\n";
             if(decideflag == true)
             {
            	 int y;
            	 if(t.has_write)
            	 {
            		 for(int j=0;j<t.var_access.size();j++)
            		 {
            			 y=t.var_access.get(j);
            		 for(int i=1;i<site1.length;i++)
            		 {
            			 if(site1[i].present[y] == true)
            			 {
            			 endString+=t.writeAtCommit(site1[i]);
            			 endString+="\n";
            			 site1[i].written[y]=false;
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

            			 for(int i=1;i<site1.length;i++)
            		    {
            		         site1[i].written[x]=false;
            		    }
         		    }

            	 }
                 t.abort();
                 releaseLocks(t);
                 commitflag=false;
             }
         }
         Waitobj w3;
         for(int i=1;i<site1.length;i++)
         {

                     for(int k=0;k<site1[i].w_queue.size();k++)
                     {

                         w3=(Waitobj)(site1[i].w_queue.get(k));
                         endString += "Found waiting transaction at site "+i;
                         endString += "\n";
                         if(t.all_access.contains(w3.var))
                         {
                        	 endString += "The variable is " + w3.var;                       
                        	 endString += "\n";
                        	 endString+=callFunction(w3);
                        	 site1[i].w_queue.remove(k);
                        	 endString+="\n";
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
        	 if(site1[siteno].present[wx.var] == true)
        	 {
             callFunction(wx);
        	 tm_queue.remove(i);
        	 }
         }
     }
 }

 public class CMain
 {
    static void parseInput(TransactionManager tm,String input,ArrayList <Transaction>TransObjects,int timestamp,BufferedWriter out)
       {
    	 // Read multiple operations on same line
    	try{
          int length = input.length();
          String subTrans="";
          Integer z =0;
          String subVariable="";
          String subSite="";
          int r=0,j=0,m=0,k=0;

         //begin transaction
           if ((input.contains("begin")) && (input.contains("beginRO") == false))
           {

               //transaction

                     subTrans = input.substring(6,length-1);

                    TransObjects.add(new Transaction(subTrans,timestamp,"ReadWrite"));
           }

           //begin read only transaction
           if ((input.contains("beginRO")) || (input.contains("BEGINRO")))
           {

                         subTrans = input.substring(8,length-1);

                        TransObjects.add(new Transaction(subTrans,timestamp,"ReadOnly"));
           }

         //end transaction
           if ((input.contains("end")) || (input.contains("END")))
           {

        	   //transaction
        	   String endString="";


                     subTrans = input.substring(4,length-1);

                    for(int w=0;w<TransObjects.size();w++)
                      {
                        Transaction temp = TransObjects.get(w);
                          if(temp.name.equals(subTrans))
                          {
                              endString+=tm.end(TransObjects.get(w));
                              out.write(endString);
                              out.newLine();
                              TransObjects.remove(temp);
                          }
                      }
           }

         //fail site
           if ((input.contains("fail")) || (input.contains("FAIL")))
           {

               //site

                     subSite = input.substring(5,length-1);
                     z = Integer.parseInt(subSite);


                    Waitobj temp[]=tm.site1[z].fail();
                    int c1=0;

                    Transaction tx;
                    for(int b=0;b<TransObjects.size();b++)
                    {
                    	tx=TransObjects.get(b);
                    	if(tx.site_access.contains(z))
                    	{
                    		tx.aborted=true;
                    	}
                    }
                    while(temp[c1]!=null)
                    {
                    	tm.callFunction(temp[c1]);
                    	c1++;
                    }
           }

         //recover site
           if ((input.contains("recover")) || (input.contains("RECOVER")))
           {

               //site

                     subSite = input.substring(8,length-1);
                     z = Integer.parseInt(subSite);

                    tm.site1[z].recover();
                    tm.waitrecovery(z);
           }

         //querystate
           if ((input.contains("querystate")) || (input.contains("queryState")))
           {
        	   String querystateString="";
                   querystateString+=tm.querystate();
                   out.write(querystateString);
                   out.newLine();

           }

         //dump() gives the committed values of all copies of all variables at all
          // sites, sorted per site.
           if (length <= 6 && ((input.contains("dump()")) || (input.contains("DUMP()"))) )
           {
        	   String dumpString="";
              dumpString +=tm.dump();
              out.write(dumpString);
              out.newLine();
           }

           //dump(i) gives the committed values of all copies of all variables at site i
           if (length > 6 && (input.contains("dump(x") == false) && (input.contains("DUMP(x") == false)
                 &&  ((input.contains("dump")) || (input.contains("DUMP")))    )
           {

        	   String dumpSiteString="";
               subSite = input.substring(5,length-1);
                 z = Integer.parseInt(subSite);

                   Site s = new Site(z);
                   dumpSiteString +=tm.dump(s);
                   out.write(dumpSiteString);
                   out.newLine();
           }

           //dump(xj) gives the committed values of all copies of variable xj at all sites.
           if ((input.contains("dump(x")) || (input.contains("DUMP(x")))
           {

        	   String dumpVariableString="";
               for(j=0;j< length -1 ; j++)
                {
                    if (input.charAt(j) == 'x' )
                    {
                           break;
                    }
                }

                 subVariable = input.substring(j,length-1);

                   Variable v =new Variable(subVariable);
                   dumpVariableString +=tm.dump(v);
                   out.write(dumpVariableString);
                   out.newLine();
           }

           //r(T,variable) operation
           if (input.startsWith("R("))
           {

        	   String readString="";
                //transaction


                for (r=0;r<length-1;r++)
                {
                    if (input.charAt(r) == ',')
                    {
                        break;
                    }
                }

                     subTrans = input.substring(2,r);

                //variable


                for(j=0;j< length -1 ; j++)
                {
                    if (input.charAt(j) == 'x' )
                    {
                           break;
                    }
                }

                 subVariable = input.substring(j+1,length-1);

                 for(int w=0;w<TransObjects.size();w++)
                 {
                       Transaction temp = (Transaction) TransObjects.get(w);
                     if(temp.name.equals(subTrans))
                     {
                         readString+=tm.read(temp,Integer.parseInt(subVariable));
                         out.write(readString);
                         out.newLine();
                     }
                 }
           }

           //W(T,variable,value) operation
           if (input.startsWith("W("))
           {

               //Transaction
        	   String writeString="";

                for (r=0;r<length-1;r++)
                {
                    if (input.charAt(r) == ',')
                    {
                        break;
                    }
                }


                     subTrans = input.substring(2,r);

                //variable


                for(j=0;j< length -1 ; j++)
                {
                    if (input.charAt(j) == 'x' )
                    {
                        break;
                    }
                }


                for(k=j;k< length -1 ; k++)
                {
                    if (input.charAt(k) == ',' )
                    {
                        break;
                    }
                }

                 subVariable = input.substring(j+1,k);

                //variable value

                for(m=length-1;m>0;m--)
                {
                    if (input.charAt(m) == ',')
                    {
                        break;
                    }
                }

                    String sub = input.substring(m+1,length-1);
                    z = Integer.parseInt(sub);

                    for(int w=0;w<TransObjects.size();w++)
                    {
                        Transaction temp=(Transaction) TransObjects.get(w);
                        if(temp.name.equals(subTrans))
                        {
                        writeString+=tm.write(temp,Integer.parseInt(subVariable),z);
                        out.write(writeString);
                        out.newLine();
                        }
                    }

           }
  	 }
  	 catch (Exception e){
  		   System.err.println("Error: " + e.getMessage());
  		   }

     }
  public static void main(String args[])
   {

      TransactionManager tm=new TransactionManager();

   try{

   File f1=new File("C:\\testcases\\sitefail.txt");  // Input file
   if(!f1.exists()){
   f1.createNewFile();
   System.out.println("No file present");
   return;
   }

   File f2=new File("C:\\testcases\\Output.txt");     // Output file


   if(!f2.exists())                             // If output file does not exist create it
   {
    f2.createNewFile();
   }


   FileWriter writer = new FileWriter(f2);
   BufferedWriter out = new BufferedWriter(writer);

   FileInputStream fstream = new FileInputStream(f1);

   DataInputStream in = new DataInputStream(fstream);
   BufferedReader br = new BufferedReader(new InputStreamReader(in));
   String strLine;
   ArrayList<String> strLineArray = new ArrayList<String>(100);
   ArrayList<Transaction> TransObjects = new ArrayList<Transaction>(200);

   int timestamp = 0;
   String subTrans="";
   String subSite="";
   String subVariable="";
   Integer z =0,z1=0;
   int r=0,m=0,j=0,k=0;


   while ((strLine = br.readLine()) != null)   {

       strLineArray.add(strLine);


   }
   for(int w=0;w<strLineArray.size();w++)
   {
       if (strLineArray.get(w).startsWith(" ") && strLineArray.get(w).endsWith(" ") )
       {
          strLineArray.remove(w);
       }

   }



   for(int i=0;i<strLineArray.size();i++)
   {
       int length = strLineArray.get(i).length();


       //parses the input on the multiple lines
       if(strLineArray.get(i).contains(";"))
       {
           timestamp++;
           String str = strLineArray.get(i);
           String delims = "[;]";
           String[] tokens = str.split(delims);

           for (int n = 0; n < tokens.length; n++)
           {

                parseInput(tm,tokens[n],TransObjects,timestamp,out);

           }
       }
       else
       {


       //begin transaction
       if ((strLineArray.get(i).contains("begin")) && (strLineArray.get(i).contains("beginRO") == false))
       {
           timestamp++;
           //transaction


                 subTrans = strLineArray.get(i).substring(6,length-1);

                 TransObjects.add(new Transaction(subTrans,timestamp,"ReadWrite"));



       }

       //begin read only transaction
     if ((strLineArray.get(i).contains("beginRO")) || (strLineArray.get(i).contains("BEGINRO")))
       {
             timestamp++;

                     subTrans = strLineArray.get(i).substring(8,length-1);

                    TransObjects.add(new Transaction(subTrans,timestamp,"ReadOnly"));


       }

     //end transaction
       if ((strLineArray.get(i).contains("end")) || (strLineArray.get(i).contains("END")))
       {
    	   timestamp++;
           //transaction
           String endString="";


                 subTrans = strLineArray.get(i).substring(4,length-1);

                 for(int w=0;w<TransObjects.size();w++)
                 {
                     Transaction temp = (Transaction) TransObjects.get(w);
                     if(temp.name.equals(subTrans))
                     {

                          endString += tm.end(temp);
                          out.write(endString);
                          out.newLine();
                          TransObjects.remove(temp);
                     }
                 }
       }

     //fail site
       if ((strLineArray.get(i).contains("fail")) || (strLineArray.get(i).contains("FAIL")))
       {
           timestamp++;
           //site

                 subSite = strLineArray.get(i).substring(5,length-1);
                 z = Integer.parseInt(subSite);

                Waitobj temp[]=tm.site1[z].fail();
                int c=0;

                Transaction tx;
                for(int b=0;b<TransObjects.size();b++)
                {
                	tx=TransObjects.get(b);
                	if(tx.site_access.contains(z))
                	{
                		tx.aborted=true;
                	}
                }
                while(temp[c]!=null)
                {
                	tm.callFunction(temp[c]);
                	c++;
                }

       }

     //recover site
       if ((strLineArray.get(i).contains("recover")) || (strLineArray.get(i).contains("RECOVER")))
       {
           timestamp++;
           //site

                 subSite = strLineArray.get(i).substring(8,length-1);
                 z = Integer.parseInt(subSite);

                 tm.site1[z].recover();
                 tm.waitrecovery(z);
       }

       //querystate
       if ((strLineArray.get(i).contains("querystate")) || (strLineArray.get(i).contains("queryState")))
       {
    	   timestamp++;
    	   String qsString="";

              qsString += tm.querystate();
              out.write(qsString);
              out.newLine();
       }

     //dump() gives the committed values of all copies of all variables at all
      // sites, sorted per site.
       if (length <= 6 && ((strLineArray.get(i).contains("dump()")) || (strLineArray.get(i).contains("DUMP()"))) )
       {
    	   timestamp++;
           String dumpString="";

               dumpString +=tm.dump();
               out.write(dumpString);
               out.newLine();
       }

       //dump(i) gives the committed values of all copies of all variables at site i
       if (length > 6 && (strLineArray.get(i).contains("dump(x") == false) && (strLineArray.get(i).contains("DUMP(x") == false)
             &&  ((strLineArray.get(i).contains("dump")) || (strLineArray.get(i).contains("DUMP")))    )
       {
    	   timestamp++;
           String dumpSiteString="";


           subSite = strLineArray.get(i).substring(5,length-1);
             z = Integer.parseInt(subSite);

                Site s = new Site(z);
                dumpSiteString +=tm.dump(s);
                out.write(dumpSiteString);
                out.newLine();
       }

       //dump(xj) gives the committed values of all copies of variable xj at all sites.
       if ((strLineArray.get(i).contains("dump(x")) || (strLineArray.get(i).contains("DUMP(x")))
       {
    	   timestamp++;
           String dumpVariableString="";

           for(j=0;j< length -1 ; j++)
            {
                if (strLineArray.get(i).charAt(j) == 'x' )
                {
                       break;
                }
            }

             subVariable = strLineArray.get(i).substring(j,length-1);

                 Variable v =new Variable(subVariable);
                 dumpVariableString+=tm.dump(v);
                 out.write(dumpVariableString);
                 out.newLine();
       }
       //r(T,variable) operation
       if (strLineArray.get(i).startsWith("R("))
       {

           timestamp++;
            //transaction
           String readString="";

            for (r=0;r<length-1;r++)
            {
                if (strLineArray.get(i).charAt(r) == ',')
                {
                    break;
                }
            }

                 subTrans = strLineArray.get(i).substring(2,r);

            //variable


            for(j=0;j< length -1 ; j++)
            {
                if (strLineArray.get(i).charAt(j) == 'x' )
                {
                       break;                   }
                    }

             subVariable = strLineArray.get(i).substring(j+1,length-1);

             for(int w=0;w<TransObjects.size();w++)
             {
                   Transaction temp = (Transaction) TransObjects.get(w);
                 if(temp.name.equals(subTrans))
                 {
                     readString+=tm.read(temp,Integer.parseInt(subVariable));
                     out.write(readString);
                     out.newLine();
                 }
             }


       }

       //W(T,variable,value) operation
       if (strLineArray.get(i).startsWith("W("))
       {
           timestamp++;
           //Transaction
           String writeString="";

            for (r=0;r<length-1;r++)
            {
                if (strLineArray.get(i).charAt(r) == ',')
                {
                    break;
                }
            }

                 subTrans = strLineArray.get(i).substring(2,r);

            //variable


            for(j=0;j< length -1 ; j++)
            {
                if (strLineArray.get(i).charAt(j) == 'x' )
                {
                    break;
                }
            }


            for(k=j;k< length -1 ; k++)
            {
                if (strLineArray.get(i).charAt(k) == ',' )
                {
                    break;
                }
            }

             subVariable = strLineArray.get(i).substring(j+1,k);

            //variable value

            for(m=length-1;m>0;m--)
            {
                if (strLineArray.get(i).charAt(m) == ',')
                {
                    break;
                }
            }

                String sub = strLineArray.get(i).substring(m+1,length-1);
                z1 = Integer.parseInt(sub);


                for(int w=0;w<TransObjects.size();w++)
                {
                    Transaction temp=(Transaction) TransObjects.get(w);
                    if((temp.name).equals(subTrans))
                    {
                    writeString+=tm.write(temp,Integer.parseInt(subVariable),z1);
                    out.write(writeString);
                    out.newLine();

                    }
                }
       }
       }

   }

   in.close();
   out.close();

     }catch (Exception e){
   System.err.println("Error: " + e.getMessage());
   }
   
   }
 }





