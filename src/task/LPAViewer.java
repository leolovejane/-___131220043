package task;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LPAViewer {
public static class LPAViewerMapper extends Mapper<Object,Text,Text,Text>{
		
		public void map(Object key,Text value, Context context) 
				throws IOException, InterruptedException{
			String[] infos=value.toString().split("\t")[0].split("#");
			context.write(new Text(infos[1]),new Text(infos[0]));
		}
	}
	public static class LPAViewerReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException{
			for (Text value:values)
				context.write(key,value);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"LPAViewer");
		job.setJarByClass(LPAViewer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(LPAViewerMapper.class);
		job.setReducerClass(LPAViewerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
