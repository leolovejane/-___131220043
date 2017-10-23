package task;


public class Driver {
	public static void main(String[] args) throws Exception{
		if (args.length==2) {
			String[] args1={args[0],"output1",args[1]};
			String[] args2={"output1","output2"};
			String[] args3={"output2","output3"};
			String[] args4={"output3","output4"};
			String[] args5={"output3","output5"};
			Job1.main(args1);
			Job2.main(args2);
			Job3.main(args3);
			PageRankDriver.main(args4);
			LPADriver.main(args5);
		}
	}
}
