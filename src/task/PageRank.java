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


public class PageRank {
	public static class PageRankMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value, Context context) 
				throws IOException, InterruptedException{
			String[] tuple=value.toString().split("\t");
			String[] page=tuple[0].split("#");
			String pageKey=page[0];
			double pr=1;
			if (page.length==2)
				pr=Double.parseDouble(page[1]);
			String[] linkPages=tuple[1].split("\\|");
			for (String linkPage:linkPages){
				if (linkPage.length()>0){
					String[] infos=linkPage.split(",");
					String linkPageKey=infos[0];
					String prValue=pageKey+"#"+String.valueOf(pr*Double.parseDouble(infos[1]));
					context.write(new Text(linkPageKey), new Text(prValue));
				}
			}
			context.write(new Text(pageKey), new Text(tuple[1]));
		}
	}
	public static class PageRankReducer extends Reducer<Text,Text,Text,Text>{
		private static double d=0.85;
		public void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException{
			String links="";
			double pagerank=0;
			for (Text value:values){
				String tmp=value.toString();
				if (tmp.startsWith("|")){
					links=tmp;
					continue;
				}
				String[] tuple=tmp.split("#");
				if (tuple.length>1)
					pagerank+=Double.parseDouble(tuple[1]);
			}
			pagerank=1.0-d+d*pagerank;
			context.write(new Text(key.toString()+"#"+String.valueOf(pagerank)),new Text(links));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"PageRank");
		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);		
	}
}
