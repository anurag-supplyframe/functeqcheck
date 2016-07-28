package functeqcheck;

import java.io.IOException;
import java.util.HashSet;
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


/*This program checks whether the two parts are fff or not
 * You can also filter based on category of part
 * 
 * The parameters that have to be passed to the job -Dcategory1 and -Dcategory2
 * 
 hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar functeqcheck.CheckIfFuncEq \
 -Dmapred.reduce.tasks=10 -Ddfs.replication=1 -Dcategory1="Microcontrollers and Processors" -Dcategory2="Microcontrollers and Processors" \
 /prod/partsio/fff/ /user/amishra/related_parts_summary_2015_16/ \
 /user/amishra/L1reln
  * 
 //Change #1
hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar functeqcheck.CheckIfFuncEq \
-Dmapred.reduce.tasks=20 -Ddfs.replication=1 -Dcategory1="Microcontrollers and Processors" -Dcategory2="Microcontrollers and Processors" \
/user/amishra/fff_pairwise /user/amishra/1year/related_parts_summary_15_16/ \
/user/amishra/1year/L1reln
 
 */

/*
 * #1	Map from /user/amishra/fff_pairwise instead of /prod/partsio/fff/
 */
public class CheckIfFuncEq extends Configured implements Tool {

	
	private static final int REF = 0;
	private static final int APP = 1;
	

	
	
	public static void main(String[] args)  throws Exception {
		int ret = ToolRunner.run(new Configuration(), new CheckIfFuncEq(), args);
		System.exit(ret);
	}
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		
	    conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.SnappyCodec");
		
		conf.set("mapred.output.compress", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
		//some debug info
		System.out.println(conf.get("category1", ""));
		System.out.println(conf.get("category2", ""));
		
		
		Job job = new Job(conf);
		job.setJarByClass(CheckIfFuncEq.class);
		job.setJobName("CheckIfFuncEq");
		
		//Start Change #1
		//MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FFFMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FFFPartsMapper.class);
		//End Change #1
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SummaryMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(FieldIntPair.class);
	    job.setMapOutputValueClass(FieldIntPair.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setGroupingComparatorClass(FieldIntPair.GroupComparator.class);
	    job.setPartitionerClass(FieldIntPair.KeyPartitioner.class);
	    job.setReducerClass(CheckReducer.class);
	    

		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	public static class CheckReducer extends Reducer<FieldIntPair, FieldIntPair, Text, NullWritable>{

		
		@Override
		protected void reduce(FieldIntPair key, Iterable<FieldIntPair> vals,Reducer<FieldIntPair, FieldIntPair, Text, NullWritable>.Context context) throws IOException, InterruptedException {

			String decision = "N" /*, currGroup = ""*/; //change #1
			Iterator<FieldIntPair> itr = vals.iterator();
			FieldIntPair val = null;
			while (itr.hasNext()) {
				val = itr.next();
				if (val.mark.get() == REF) {
					decision = "Y";
					//currGroup = val.field.get("group"); //Change #1
				} else {
					if("Y".equals(decision)){
						context.getCounter("CheckReducer", "FFF.SearchPart").increment(1);
					}
					
					context.write(new Text(key.field.get("part1") + "\t" + key.field.get("part2") + "\t"
							+ val.field.get("weight") + "\t" + val.field.get("category1") + "\t"
							+ val.field.get("category2") + "\t" + decision), NullWritable.get());
				}

			}

		}
	}
	
	public static class SummaryMapper extends Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>{
		private FieldIntPair keyOut = new FieldIntPair( new FieldWritable(
				"part1" + "\t" + "part2"), 
				APP);
		private FieldIntPair valOut = new FieldIntPair( new FieldWritable(
				"weight" + "\t" + "category1" + "\t" + "category2"), 
				APP);
		
		private String cat1="", cat2="";
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context) throws IOException, InterruptedException {
			cat1=context.getConfiguration().get("category1", "");
			cat2=context.getConfiguration().get("category2", "");
			if("".equals(cat1) || "".equals(cat2)){
				throw new RuntimeException("category of both parts not specified");
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context)	throws IOException, InterruptedException {
			
			String[] vals=value.toString().split("\t");
			

			
			if ( !( 
					cat1.equals(vals[3]) && cat2.equals(vals[4]) 
					) ){
				//ignore all pairs where both parts are not MUC
				return;

			}
			context.getCounter("SummaryMapper", "valid.categories").increment(1);
			
			keyOut.field.set(vals[0].replaceAll("[^a-zA-Z0-9]", "").toUpperCase() + "\t" 
			+ vals[1].replaceAll("[^a-zA-Z0-9]", "").toUpperCase());
			//remove all non-alphanumeric characters and convert to upper case
			valOut.field.set(vals[2]+"\t"+vals[3]+"\t"+vals[4]);
			
			context.write(keyOut, valOut);
			context.progress();
			
		}
		
	}
	
	/*
	 * This class reads from the raw fff data 
	 */
	public static class FFFMapper extends Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>{
		
		private FieldIntPair keyOut = new FieldIntPair( new FieldWritable("part1" + "\t" + "part2"), REF);
		private FieldIntPair valOut = new FieldIntPair( new FieldWritable("group"), REF);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context) throws IOException, InterruptedException {
			
			
			String[] catAndPartStr = value.toString().split("\\s+");
			String grp=catAndPartStr[0];
			String[] parts = catAndPartStr[1].split("\u0007");
			
			HashSet<String> filteredPartsSet = new HashSet<String>(parts.length/2);
			
			for(int i=0 ; i < parts.length ; i += 2 ){
				filteredPartsSet.add(parts[i+1].replaceAll("[^a-zA-Z0-9]", "").toUpperCase());
				//remove all non-alphanumeric characters and convert to upper case
				
			}
			
			String[] filteredParts = new String[filteredPartsSet.size()];
			filteredPartsSet.toArray(filteredParts);
			
			
			
			
			for(int i = 0 ; i< filteredParts.length ; i++){
				for(int j=0 ; j< filteredParts.length ; j++){
					if(i!=j){
						keyOut.field.set(filteredParts[i]+"\t"+filteredParts[j]);
						valOut.field.set(grp);
						context.write(keyOut, valOut);
					}
				}
				
			}
			context.getCounter("CheckIfFuncEq.FFFMapper", "groups.processed").increment(1);
			context.progress();
			
		}
	}
	
	
	//Start Change #1
	/*
	 * This class reads from fff_pairwise which means we have already created pairs from the raw fff data
	 */
	public static class FFFPartsMapper extends Mapper<LongWritable , Text, FieldIntPair , FieldIntPair >{
		
		private FieldIntPair keyOut = new FieldIntPair(new FieldWritable(
					"part1" + "\t" + "part2" 
		
					),																	REF);
		
		private FieldIntPair valOut = new FieldIntPair(new FieldWritable("dummy"), 		REF);
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context)
				throws IOException, InterruptedException {
			
			String[] toks=value.toString().split("\t");

			
			keyOut.field.set(value);
			valOut.field.set("Y");
			context.write(keyOut, valOut);
		}
		
		
	}
	//End Change #1
	

}
