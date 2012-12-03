package com.code;
import java.util.Vector;

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