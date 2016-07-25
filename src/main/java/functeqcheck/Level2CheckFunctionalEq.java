package functeqcheck;

/*
hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
functeqcheck.Level2CheckFunctionalEq \
-Dmapred.reduce.tasks=10 -Ddfs.replication=1 \
/user/amishra/fff_pairwise/ /user/amishra/L2relnGroup_MUC \
/user/amishra/L2reln

 */



import java.io.IOException;
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





import supplyframe.utils.FieldIntPair;

public class Level2CheckFunctionalEq extends Configured implements Tool {

	private static final int REF = 0;
	private static final int APP = 1;
	
	
	public static void main(String[] args) throws Exception {
		int rc=ToolRunner.run(new Configuration(), new Level2CheckFunctionalEq() , args);
		System.exit(rc);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf= getConf();
		
		
		Job job = new Job(conf);
		job.setJarByClass(Level2CheckFunctionalEq.class);
		job.setJobName("Level2CheckFunctionalEq");
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FFFMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, L2EqMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(FieldIntPair.class);
	    job.setMapOutputValueClass(FieldIntPair.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setGroupingComparatorClass(FieldIntPair.GroupComparator.class);
	    job.setPartitionerClass(FieldIntPair.KeyPartitioner.class);
	    job.setReducerClass(L2EqReducer.class);
	    
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	
	public static class L2EqReducer extends Reducer<FieldIntPair, FieldIntPair, Text, NullWritable>{
		
		
		
		
		@Override
		protected void reduce(FieldIntPair key, Iterable<FieldIntPair> vals, Reducer<FieldIntPair, FieldIntPair, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String decision = "N";
			Iterator<FieldIntPair> itr = vals.iterator();
			FieldIntPair val = null;
			while (itr.hasNext()) {
				val = itr.next();
				if (val.mark.get() == REF) {
					decision = "Y";
					
				} else {
					if("Y".equals(decision)){
						context.getCounter("L2EqReducer", "FFF.SearchPart").increment(1);
					}
					
					context.write(new Text(key.field.get("part1") + "\t" + key.field.get("part2") + "\t" + val.field.get("weight") + "\t" 
							+ decision), NullWritable.get());
				}

			}
			
		}
	}
	
	public static class FFFMapper extends Mapper<LongWritable, Text , FieldIntPair ,  FieldIntPair>{
		private FieldIntPair keyOut = new FieldIntPair( new FieldWritable(
				"part1" + "\t" + "part2"), 
				REF);
		
		private FieldIntPair valOut = new FieldIntPair( new FieldWritable(
				"dummy"), 
				REF);
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context)throws IOException, InterruptedException {
			
			String[] tokens=value.toString().split("\t");
			String part1=tokens[0].replaceAll("[^a-zA-Z0-9]", "").toUpperCase();
			String part2=tokens[1].replaceAll("[^a-zA-Z0-9]", "").toUpperCase();
			
			keyOut.field.set(new Text(part1 + "\t" + part2));
			valOut.field.set("Y");
			context.write(keyOut, valOut);
			
			
		}
		
	}
	
	public static class L2EqMapper extends Mapper<LongWritable, Text, FieldIntPair ,  FieldIntPair>{
		
		private FieldIntPair keyOut = new FieldIntPair( new FieldWritable(
				"part1" + "\t" + "part2"), 
				APP);
		
		private FieldIntPair valOut = new FieldIntPair( new FieldWritable(
				"weight"), 
				APP);
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context) throws IOException, InterruptedException {

			
			String[] tokens=value.toString().split("\t");
			
			String part1  = tokens[0].replaceAll("[^a-zA-Z0-9]", "").toUpperCase() ;
			String part2  = tokens[1].replaceAll("[^a-zA-Z0-9]", "").toUpperCase();
			
			String weight = tokens[2];
			
			
			keyOut.field.set(part1+"\t"+part2);
			valOut.field.set(weight);
			
			context.write(keyOut, valOut);
			
			
		}
	}
	
	
	
	
}
