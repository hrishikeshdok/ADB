package com.RCCR;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class Simulator {


	static int count = 0;
	
	public static void main(String arg[])
	{
		try{

		for(int i=1; i<=10; i++)
		{
			TransactionManager.sites[i] = new Site(i, count);
			TransactionManager.siteStatus[i] = true;
		}
		File fromFile = new File("inputData.txt");
		//File toFile = new File(output.txt);
		BufferedReader reader = new BufferedReader(new FileReader(fromFile));
        //BufferedWriter writer = new BufferedWriter(new FileWriter(toFile));
        String line = null;
        String delimiter = "; ";//seperator between two concurrent commands
        String command[];
        
        String commandName = "";
        String transactionName = "";
        String variableName = "";
        String variableValue = "";
        String siteName = "";
        
        while ((line=reader.readLine()) != null)
        {
        	
			if(line.charAt(0) != '/')//escape comments
			{
				command = line.split(delimiter);
				for(int i=0; i<command.length; i++)
    			{
					String [] splittedCommand = command[i].split("\\(|\\)");
					commandName = splittedCommand[0];
					String parameters = splittedCommand[1];
					String [] arguments;
					if(!parameters.isEmpty() && parameters  != null)
					{
						arguments = parameters.split(",");
						
						//parse input parameters
						switch (arguments.length) {
						case 1:
							transactionName = arguments[0];
							siteName = arguments[0];
							variableName = arguments[0].replaceAll("x", "");
							break;
						case 2:
							transactionName = arguments[0];
							variableName = arguments[1].replaceAll("x", "");
							break;
						case 3:
							transactionName = arguments[0];
							variableName = arguments[1].replaceAll("x", "");
							variableValue = arguments[2];
							break;
							
						default:
							break;
						}
					}
					
					if(commandName.equals("begin"))
						TransactionManager.begin(transactionName, false);
					else if ( commandName.equals("beginRO") )
						TransactionManager.begin(transactionName, true);
					else if(commandName.equals("dump"))
					{
						if(siteName.equals(""))
							TransactionManager.dump();
						else if(siteName.contains("x"))
							TransactionManager.dump(Integer.parseInt(variableName));
						else
							TransactionManager.dump(siteName);
					}
					else if(commandName.equals("R"))
						TransactionManager.R(transactionName, Integer.parseInt(variableName) );
					else if(commandName.equals("W"))
						TransactionManager.W(transactionName, Integer.parseInt(variableName), Integer.parseInt(variableValue));
					else if(commandName.equals("fail"))
						TransactionManager.fail(Integer.parseInt(siteName));
					else if(commandName.equals("recover"))
						TransactionManager.recover(Integer.parseInt(siteName));
					else if(commandName.equals("end"))
						TransactionManager.end(transactionName);
				}
				count++;
			}
			TransactionManager.checkWaitQueue();
			TransactionManager.sendMessagesToSites();
			TransactionManager.checkBuffers();
		}
		//queryState();
		
		TransactionManager.printTransactionSummary();
		//writer.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	
	}
}
