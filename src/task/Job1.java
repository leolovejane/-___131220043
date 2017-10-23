package task;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.util.FilterModifWord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1 {
	public static class Job1Mapper extends Mapper<Object,Text,Text,Text>{
		public void setup(Context context) {
			try {
				@SuppressWarnings("deprecation")
				Path[] cacheFiles=context.getLocalCacheFiles();
				String line;
				if (cacheFiles!=null&&cacheFiles.length>0){
					BufferedReader dataReader=new BufferedReader
							(new FileReader(cacheFiles[0].toString()));
					try{
						while((line=dataReader.readLine())!=null){
							UserDefineLibrary.insertWord(line, "name", 1000);
						}
					} finally{
						dataReader.close();
					}
				}
			} catch (IOException e){
				System.err.println("Exception reading DistributedCache: "+e);
			}
		}
		
		public void map(Object key,Text value, Context context) 
				throws IOException, InterruptedException{
			List<Term> parse = FilterModifWord.modifResult(ToAnalysis.parse(value.toString()));
			StringBuilder str=new StringBuilder();
			for (Term term:parse){
				if (term.getNatureStr().equals("name")){
					str.append(term.getName());
					str.append("#");
				}
			}
			if (str.length()>0)
				context.write(new Text(str.toString()),new Text("#"));
		}
	}
	public static class Job1Reducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException{
			for (Text value:values){
				context.write(key,value);
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Job1");
		job.addCacheFile(new Path(args[2]).toUri());
		job.setJarByClass(Job1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
}
