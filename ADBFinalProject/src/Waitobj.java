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