package task;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRankViewer {
	public static class DoubleWritableDecreasingComparator extends Comparator {
	    public int compare(DoubleWritable a,DoubleWritable b){
	        return -super.compare(a, b);
	        
	    }
	    @Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	        // TODO Auto-generated method stub
	        return -super.compare(b1, s1, l1, b2, s2, l2);
	    }
	}
	
	public static class PageRankViewerMapper extends Mapper<Object,Text,DoubleWritable,Text>{
		public void map(Object key,Text value, Context context) 
				throws IOException, InterruptedException{
			String[] infos=value.toString().split("\t")[0].split("#");
			String page=infos[0];
			double pr=Double.parseDouble(infos[1]);
			context.write( new DoubleWritable(pr),new Text(page));
		}
	}
	public static class PageRankViewerReducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text>{
		public void reduce(DoubleWritable key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException{
			for (Text value:values)
				context.write(key,value);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"PageRankViewer");
		job.setJarByClass(PageRankViewer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PageRankViewerMapper.class);
		job.setReducerClass(PageRankViewerReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setSortComparatorClass(DoubleWritableDecreasingComparator.class);;
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
}
