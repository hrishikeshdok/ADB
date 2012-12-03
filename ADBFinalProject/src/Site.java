import java.util.ArrayList;
import java.util.Vector;

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