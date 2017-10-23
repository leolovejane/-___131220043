package task;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PageRankDriver {
	public static int times=15;
	public static List<Double> getList(String filePath) 
			throws Exception{
		Configuration conf=new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		FSDataInputStream in=null;
		Scanner scanner;
		Path path=new Path(filePath);
		List<Double> list=new ArrayList<>();
		in=hdfs.open(path);
		scanner=new Scanner(in);
		while (scanner.hasNext()){
			String str=scanner.nextLine();
			list.add(Double.parseDouble(str.split("#|\t")[1]));
		}
		scanner.close();
		in.close();
		hdfs.close();
		return list;
	}
	public static boolean judge(String filePath1,String filePath2) 
			throws Exception{
		List<Double> list1,list2;
		list1=getList(filePath1);
		list2=getList(filePath2);
		int size=list1.size();
		double max=Double.NEGATIVE_INFINITY;
		for (int i=0;i<size;i++){
			double diff=Math.abs(list1.get(i)-list2.get(i));
			if (diff>max) max=diff;
		}
		if (max<0.05) return true;
		else return false;
	}
	public static void main(String[] args) throws Exception{
		String[] argsDetail={args[0],args[1]+"/Data0"};
		for (int i=0;i<times;i++){
			PageRank.main(argsDetail);
			String[] temp={"",""};
			temp[0]=argsDetail[0]+"/part-r-00000";
			temp[1]=argsDetail[1]+"/part-r-00000";
			argsDetail[0]=args[1]+"/Data"+i;
			argsDetail[1]=args[1]+"/Data"+String.valueOf(i+1);
			if (i>0){
				if (judge(temp[0],temp[1])) break;
			}
		}
		argsDetail[1]=args[1]+"/FinalRank";
		PageRankViewer.main(argsDetail);
	}
}
