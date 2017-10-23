package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3 {
	public static class Job3Mapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value, Context context) 
				throws IOException, InterruptedException{
			String[] words=value.toString().split("#|\t");
			context.write(new Text(words[0]), new Text(words[1]+"#"+words[2]));
		}
	}
	public static class Job3Reducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException{
			List<String> sList=new ArrayList<>();
			List<Integer> iList=new ArrayList<>();
			StringBuilder result=new StringBuilder();
			int sum=0;
			for (Text value:values){
				String[] words=value.toString().split("#");
				sList.add(words[0]);
				int temp=Integer.parseInt(words[1]);
				iList.add(temp);
				sum+=temp;
			}
			for (int i=0;i<sList.size();i++){
				double p=(double)iList.get(i)/(double)sum;
				result.append("|"+sList.get(i)+","+p);
			}
			context.write(key, new Text(result.toString()));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Job3");
		job.setJarByClass(Job3.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Job3Mapper.class);
		job.setReducerClass(Job3Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
}
