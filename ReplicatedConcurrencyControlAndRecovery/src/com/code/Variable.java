package com.code;
import java.util.Vector;

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