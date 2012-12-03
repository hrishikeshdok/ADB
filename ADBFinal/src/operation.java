
public class operation {


	int variableName;
	char operationType;// R/W
	Transaction transactionName;
	int value;//if a write operation
	operation nextOperation;
	boolean transactionActive;//determines if transaction is active and operation is to be executed or not

	operation()
	{}

	operation(int variableName, char operationType, Transaction transactionName, int value)
	{
		this.variableName = variableName;
		this.operationType = operationType;
		this.transactionName = transactionName;
		this.value = value;
		nextOperation = null;
		transactionActive = true;
	}

	
}
