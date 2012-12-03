package Test;

public class Test {
	
	public static void main(String args[])
	{
		String str = "dump()";
		
		String [] test = str.split("\\(|\\)");
		
		for (String string : test) {
			System.out.println(string);
		} 
		
		System.out.println(test[1]);
		//System.out.println(test[2]);
		
	}

}
