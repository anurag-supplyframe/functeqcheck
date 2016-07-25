package functeqcheck;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/*
 * 
hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar functeqcheck.Level2Relation \
-Dmapred.reduce.tasks=10 -Ddfs.replication=1 \
-Dcategory="Microcontrollers and Processors" \
/user/amishra/related_parts_summary_2015_16 /user/amishra/L2relnGroup_MUC
 */

public class Level2Relation extends Configured implements Tool{

	private static final int REF = 0;
	private static final int APP = 1;
	
	
	public static void main(String[] args)  throws Exception {
		int ret = ToolRunner.run(new Configuration(), new Level2Relation(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		//some debug info
		System.out.println(conf.get("category", ""));
	
		
		Job job = new Job(conf);
		job.setJarByClass(Level2Relation.class);
		job.setJobName("Level2Relation");


		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, L2Mapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(FieldWritable.class);
	    job.setMapOutputValueClass(FieldWritable.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    

	    job.setReducerClass(L2Reducer.class);
	    

		
		return job.waitForCompletion(true) ? 0 : 1;
		
		
		
	}
	
	public static class L2Reducer extends Reducer<FieldWritable, FieldWritable, Text, NullWritable>{
		
		private String cat="";
		
		@Override
		protected void setup(Reducer<FieldWritable, FieldWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			cat=context.getConfiguration().get("category", "");
			
			if("".equals(cat) ){
				throw new RuntimeException("Category of joining part for l2 relation  not specified");
			}
		}
		
		
		//the list of values that we will get will have the same category and weight is that of the part to the common key
		@Override
		protected void reduce(FieldWritable key, Iterable<FieldWritable> vals,Reducer<FieldWritable, FieldWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			Iterator<FieldWritable> itr=vals.iterator();
			HashMap<String, String> l2PartsMap= new HashMap<String, String>();
			while(itr.hasNext()){
				FieldWritable field=itr.next();
				l2PartsMap.put(field.get("part1") , field.get("weight"));
				
			}
			//the above map will have all the parts which are of the same category and are connected by a common part
			//belonging to another category
//			StringBuilder sb = new StringBuilder();
//			Iterator<Entry<String, String>> itr2=l2PartsMap.entrySet().iterator();
//			while(itr2.hasNext()){
//				Entry<String, String> a=itr2.next();
//				sb.append(a.getKey()+"\t"+a.getValue());
//			}
//			context.write(new Text(sb.toString()), NullWritable.get());
			
			String[] partArr=new String[l2PartsMap.size()] ;
			l2PartsMap.keySet().toArray(partArr) ;
			
			for(int i=0 ; i<partArr.length ; i++){
				for (int j = 0; j < partArr.length; j++) {
					
					if(i!=j){
						context.write(
								new Text(
										partArr[i]+ "\t" +  
										partArr[j] + "\t" +
										l2PartsMap.get(partArr[i]) + ":" + l2PartsMap.get(partArr[j]) 
										), 
								NullWritable.get());
					}
				}
			}
			
			
		}
	}
	
	
	public static class L2Mapper  extends Mapper<LongWritable, Text, FieldWritable, FieldWritable>{
		private FieldWritable keyOut =  new FieldWritable(
				"part2");
		private FieldWritable valOut =  new FieldWritable(
				"part1" + "\t"+ "weight");
		
		private String cat="";
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, FieldWritable, FieldWritable>.Context context)
				throws IOException, InterruptedException {
			cat=context.getConfiguration().get("category", "");
			
			if("".equals(cat) ){
				throw new RuntimeException("Category of joining part for l2 relation  not specified");
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FieldWritable, FieldWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] vals=value.toString().split("\t");
			
			if ( ! 	cat.equals(vals[3])	){
				return;//the first part is not a muc, so return
			}
			if( cat.equals(vals[4]) ){
				return;//the second part is a muc, so return
			}
			
			//we are left with records where the first part is a muc and second is not a muc
			keyOut.set(vals[1]);//the non-muc is the key
			
			valOut.set(vals[0]+"\t"+vals[2]);
			
			context.write(keyOut, valOut);
			context.progress();
			
		}
		
	}
	

	
}
