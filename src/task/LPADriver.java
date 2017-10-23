package task;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LPADriver {
	private static int times=15;
	public static List<String> getLabelList(String filePath) 
			throws Exception{
		Configuration conf=new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		FSDataInputStream in=null;
		Scanner scanner;
		Path path=new Path(filePath);
		List<String> list=new ArrayList<>();
		in=hdfs.open(path);
		scanner=new Scanner(in);
		while (scanner.hasNext()){
			String str=scanner.nextLine();
			list.add(str.split("#|\t")[1]);
		}
		scanner.close();
		in.close();
		hdfs.close();
		return list;
	}
	public static boolean judgeLabel(String filePath1,String filePath2) 
			throws Exception{
		List<String> list1,list2;
		boolean flag=true;
		list1=getLabelList(filePath1);
		list2=getLabelList(filePath2);
		int size=list1.size();
		for (int i=0;i<size;i++){
			if (!list1.get(i).equals(list2.get(i))) {
				flag=false;
				break;
			}
		}
		return flag;
	}
	public static void main(String[] args) throws Exception{
		String[] argsDetail={args[0],args[1]+"/Data0"};
		for (int i=0;i<times;i++){
			LPA.main(argsDetail);
			String[] temp={"",""};
			temp[0]=argsDetail[0]+"/part-r-00000";
			temp[1]=argsDetail[1]+"/part-r-00000";
			argsDetail[0]=args[1]+"/Data"+i;
			argsDetail[1]=args[1]+"/Data"+String.valueOf(i+1);
			if (i>0){
				if (judgeLabel(temp[0],temp[1])) break;
			}
		}
		argsDetail[1]=args[1]+"/FinalLabel";
		LPAViewer.main(argsDetail);
	}
}
