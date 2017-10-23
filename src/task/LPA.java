package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LPA {
	public static class LPAMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value, Context context) 
				throws IOException, InterruptedException{
			String[] tuple=value.toString().split("\t");
			String[] page=tuple[0].split("#");
			String pageKey=page[0];
			String label=page[0];
			if (page.length==2)
				label=page[1];
			String[] linkPages=tuple[1].split("\\|");
			for (String linkPage:linkPages){
				if (linkPage.length()>0){
					String[] infos=linkPage.split(",");
					String linkPageKey=infos[0];
					String weight=infos[1];
					context.write(new Text(pageKey), new Text(linkPageKey+"#"+weight));
					context.write(new Text(linkPageKey), new Text(pageKey+"#"+weight+"#"+label));
				}
			}
			context.write(new Text(pageKey), new Text(tuple[1]));
		}
	}
	public static class LPAReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException{
			Map<String,Double> map=new HashMap<>();
			Map<String,String> labelMap=new HashMap<>();
			List<String> two=new ArrayList<>();
			String links="";
			for (Text value:values){
				String tmp=value.toString();
				if (tmp.startsWith("|")){
					links=tmp;
					continue;
				}
				String[] tuple=tmp.split("#");
				if (tuple.length>2) {
					labelMap.put(tuple[0],tuple[2]);
				}
				else {
					two.add(tmp);
				}
			}
			for (String str:two){
				String[] tuple=str.split("#");
				String pageKey=tuple[0];
				double weight=Double.parseDouble(tuple[1]);
				String label=labelMap.get(pageKey);
				if (map.containsKey(label)){
					double temp=map.get(label);
					temp+=weight;
					map.put(label,temp);
				}
				else
					map.put(label, weight);
			}
			Map<String,Double> mapSort=new TreeMap<>(map);
			List<Map.Entry<String,Double>> list = new ArrayList<Map.Entry<String,Double>>(mapSort.entrySet());
	        Collections.sort(list,new Comparator<Map.Entry<String,Double>>() {
	            public int compare(Entry<String, Double> o1,
	                    Entry<String, Double> o2) {
	                return o2.getValue().compareTo(o1.getValue());
	            }
	            
	        });
	        int m=1;
	        while (m<list.size()){
	        	if (Math.abs(list.get(m).getValue()-list.get(0).getValue())<0.00000001){
	        		m++;
	        	}
	        	else break;
	        }
	        Random random=new Random();
	        int index=random.nextInt(m);
	        String label=list.get(index).getKey();
			context.write(new Text(key.toString()+"#"+label),new Text(links));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"LPA");
		job.setJarByClass(LPA.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(LPAMapper.class);
		job.setReducerClass(LPAReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
}
