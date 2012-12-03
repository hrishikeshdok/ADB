import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;



public class Simulator {
	
	static ArrayList<Transaction> transactionList = new ArrayList<Transaction>();//keeps track of active transactions
	static Site sites[] = new Site[11];
	static boolean siteStatus[] = new boolean[11];//true if up, false if down
	static operation messageArray[] = new operation[11];//one for each site
	static ArrayList<operation> waitQueue = new ArrayList<operation>();//wait queue of transaction operations
	static int count = 0;
	static ArrayList<Transaction> keepTrackOfTrans = new ArrayList<Transaction>();//keeps track of transactions to print summary at the TransactionManager.end

	
	public static void main(String arg[])
	{
		try{

		for(int i=1; i<=10; i++)
		{
			TransactionManager.sites[i] = new Site(i, count);
			TransactionManager.siteStatus[i] = true;
		}
		File fromFile = new File("tp.txt");
		//File toFile = new File(output.txt);
		BufferedReader reader = new BufferedReader(new FileReader(fromFile));
        //BufferedWriter writer = new BufferedWriter(new FileWriter(toFile));
        String line = null;
        String delimiter = "; ";//seperator between two concurrent commands
        String temp[];
        while ((line=reader.readLine()) != null)
        {
			if(line.charAt(0) != '/')//escape comments
			{
				temp = line.split(delimiter);
				for(int i=0; i<temp.length; i++)
    			{
					System.out.println(temp[i]);
					if(temp[i].indexOf("begin") != -1)
					{
						if(temp[i].indexOf("RO") != -1)
							TransactionManager.begin(temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(")")), true);
						else
							TransactionManager.begin(temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(")")), false);
					}
					else if(temp[i].indexOf("dump") != -1)
					{
						String attr = temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(")"));
						if(attr.equals(""))
							TransactionManager.dump();
						else if(attr.indexOf("x") == -1)
							TransactionManager.dump(Integer.parseInt(attr));
						else
							TransactionManager.dump(attr);
					}
					else if(temp[i].charAt(0) == 'R')
						TransactionManager.R(temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(",")), Integer.parseInt(temp[i].substring(temp[i].indexOf(",") + 2, temp[i].indexOf(")"))), temp[i]);
					else if(temp[i].charAt(0) == 'W')
						TransactionManager.W(temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(",")), Integer.parseInt(temp[i].substring(temp[i].indexOf(",") + 2, temp[i].indexOf(",", 5))), Integer.parseInt(temp[i].substring(temp[i].indexOf(",", 5) + 1, temp[i].indexOf(")"))), temp[i]);
					else if(temp[i].indexOf("fail") != -1)
						TransactionManager.fail(Integer.parseInt(temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(")"))));
					else if(temp[i].indexOf("recover") != -1)
						TransactionManager.recover(Integer.parseInt(temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(")"))));
					else if(temp[i].indexOf("end") != -1)
						TransactionManager.end(temp[i].substring(temp[i].indexOf("(") + 1, temp[i].indexOf(")")));
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
