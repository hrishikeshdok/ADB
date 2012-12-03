package com.code;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class TransactionSimulator
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
			//int r=0,j=0,m=0,k=0;
			String [] splittedInput = input.split("\\(|\\)");
			String [] splittedString;

			//begin transaction
			if(splittedInput[0].equalsIgnoreCase("BEGIN"))
			{
				//transaction
				subTrans = splittedInput[1];

				TransObjects.add(new Transaction(subTrans,timestamp,"ReadWrite"));
			}

			//begin read only transaction

			else if(splittedInput[0].equalsIgnoreCase("BEGINRO"))
			{
				subTrans = splittedInput[1];

				TransObjects.add(new Transaction(subTrans,timestamp,"ReadOnly"));
			}

			//end transaction
			else if(splittedInput[0].equalsIgnoreCase("END"))
			{

				//transaction
				String endString="";
				subTrans = splittedInput[1];

				for(int w=0;w<TransObjects.size();w++)
				{
					Transaction temp = TransObjects.get(w);
					if(temp.name.equals(subTrans))
					{
						endString += tm.end(TransObjects.get(w));
						out.write(endString);
						out.newLine();
						TransObjects.remove(temp);
					}
				}
			}

			//fail site
			else if(splittedInput[0].equalsIgnoreCase("FAIL"))
			{

				//site


				subSite = splittedInput[1].replaceFirst("x", "");
				z = Integer.parseInt(subSite);


				Waitobj temp[]=tm.sites[z].fail();
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
			else if(splittedInput[0].equalsIgnoreCase("RECOVER"))
			{

				//site
				subSite = splittedInput[1].replaceFirst("x", "");
				z = Integer.parseInt(subSite);

				tm.sites[z].recover();
				tm.waitrecovery(z);
			}

			//querystate
			else if(splittedInput[0].equalsIgnoreCase("QUERYSTATE"))
			{
				String querystateString="";
				querystateString+=tm.querystate();
				out.write(querystateString);
				out.newLine();

			}

			//dump() gives the committed values of all copies of all variables at all
			// sites, sorted per site.
			else if(splittedInput[0].equalsIgnoreCase("DUMP") && splittedInput.length == 1)
			{
				String dumpString="";
				dumpString += tm.dump();
				out.write(dumpString);
				out.newLine();
			}

			//dump(i) gives the committed values of all copies of all variables at site i
			else if(splittedInput[0].equalsIgnoreCase("DUMP") && splittedInput.length == 2 && !splittedInput[1].contains("x"))
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
			else if(splittedInput[0].equalsIgnoreCase("DUMP") && splittedInput.length == 2 && splittedInput[1].contains("x"))
			{

				String dumpVariableString="";



				subVariable = splittedInput[1];

				Variable v =new Variable(subVariable);
				dumpVariableString +=tm.dump(v);
				out.write(dumpVariableString);
				out.newLine();
			}

			//r(T,variable) operation
			else if(splittedInput[0].equalsIgnoreCase("R"))
			{

				String readString="";


				splittedString = splittedInput[1].split(",");
				//transaction
				subTrans = splittedString[0];
				//variable
				subVariable = splittedString[1];


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
			else if(splittedInput[0].equalsIgnoreCase("W"))
			{
				//Transaction
				String writeString="";

				splittedString = splittedInput[1].split(",");
				subTrans = splittedString[0];
				subVariable = splittedString[1].replaceAll("x", "");
				String subValue = splittedString[2];

				z = Integer.parseInt(subValue);

				for(int w=0;w<TransObjects.size();w++)
				{
					Transaction temp=(Transaction) TransObjects.get(w);
					if(temp.name.equals(subTrans))
					{
						writeString += tm.write(temp,Integer.parseInt(subVariable),z);
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

			File f1=new File("E:\\Study\\ADB\\ADB-Project-1\\sitefail.txt");  // Input file
			if(!f1.exists()){
				f1.createNewFile();
				System.out.println("No file present");
				return;
			}

			File f2=new File("E:\\Study\\ADB\\ADB-Project-1\\Output.txt");     // Output file


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
			/*  String subTrans="";
   String subSite="";
   String subVariable="";
   Integer z =0,z1=0;
   int r=0,m=0,j=0,k=0;*/


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
					parseInput(tm, strLineArray.get(i), TransObjects, timestamp, out);
				}

			}

			in.close();
			out.close();

		}catch (Exception e){
			System.err.println("Error: " + e.getMessage());
		}

	}
}





