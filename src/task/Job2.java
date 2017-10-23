package task;

import java.io.IOException;
import java.util.HashSet;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2 {
	public static class Job2Mapper extends Mapper<Object,Text,Text,IntWritable>{
		public void map(Object key,Text value, Context context) 
				throws IOException, InterruptedException{
			String[] names=value.toString().split("#|\t");
			Set<String> set=new HashSet<>();
			for (String name:names) set.add(name);
			for (String name1:set){
				for (String name2:set){
					if (!name1.equals(name2)) 
						context.write(new Text(name1+"#"+name2),new IntWritable(1));
				}
			}
		}
	}
	public static class Job2Combiner extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException{
			int sum=0;
			for (IntWritable value:values){
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class Job2Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException{
			int sum=0;
			for (IntWritable value:values){
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Job2");
		job.setJarByClass(Job2.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Job2Mapper.class);
		job.setCombinerClass(Job2Combiner.class);
		job.setReducerClass(Job2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
}
