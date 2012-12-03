
public class Variable {


	int index;
	int value;
	int commitTime;
	boolean valid;//False if site recovers and no write takes place on the variable yet
	Variable prevVar;//previous commited copy of variable

	Variable(int index, int value, int count)
	{
		this.index = index;
		this.value = value;
   		commitTime = count;
   		valid = true;
   		prevVar = null;
	}

	void invalidate()
	{
		valid = false;
	}

	void validate()
	{
		valid = true;
	}

	
}
