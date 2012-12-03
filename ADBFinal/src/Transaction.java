import java.util.ArrayList;


public class Transaction {
	
	String transactionName;
	int startTime;
	boolean readOnly;
	boolean accessedSites[];//to keep track of which sites were accessed by the transaction
	ArrayList<String> transSummary;

	Transaction()
	{}


	Transaction(String transactionName, boolean readOnly, int count)
	{
		this.transactionName = transactionName;
		this.readOnly = readOnly;
   		startTime = count;
   		accessedSites = new boolean[11];
   		transSummary = new ArrayList<String>();
	}

	public void printSummary()//prints whatever the transaction has gone through
	{
		System.out.println("\nSummary for Transaction " + transactionName + " : ");
		for(int i=0; i<transSummary.size(); i++)
			System.out.println(transSummary.get(i));
	}


}
