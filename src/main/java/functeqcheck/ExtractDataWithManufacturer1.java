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
import org.apache.hadoop.mapreduce.lib.input.FieldInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FieldOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import supplyframe.utils.FieldIntPair;




/*Populate the manufacturer1
 * 
 * 
hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
functeqcheck.ExtractDataWithManufacturer1 \
-Dmapred.reduce.tasks=10 -Ddfs.replication=1 \
-Dth_wt=10000 -Dkey_col=0 \
/user/amishra/partsio_extract/ \
/user/amishra/L1reln_extracted/ \
/user/amishra/L1reln_extracted_man1

hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
functeqcheck.ExtractDataWithManufacturer1 \
-Dmapred.reduce.tasks=10 -Ddfs.replication=1 \
-Dth_wt=10000 -Dkey_col=0 \
/user/amishra/partsio_extract/ \
/user/amishra/1year/L1reln_extracted/ \
/user/amishra/1year/L1reln_extracted_man1

 * #1 The same part can be manufactured by more than 1 manufacturer.
 * We need to enumerate all of them.
 * 
 * 
*/
public class ExtractDataWithManufacturer1 extends Configured implements Tool {

	public static final String THRESHOLD_WEIGHT = "th_wt";
	public static final String KEY_COL = "key_col";
	
	
	public static void main(String[] args) throws Exception {
		int rc=ToolRunner.run(new Configuration(), new ExtractDataWithManufacturer1(), args);
		System.exit(rc);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf=getConf();
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.SnappyCodec");
		
		
		Job job=new Job(conf);
		job.setJarByClass(ExtractDataWithManufacturer1.class);
		job.setJobName("ExtractDataWithManufacturer1");
		
		
		job.setMapOutputKeyClass(FieldIntPair.class);
		job.setMapOutputValueClass(FieldIntPair.class);
		job.setOutputKeyClass(FieldWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
	    job.setGroupingComparatorClass(FieldIntPair.GroupComparator.class);
	    job.setPartitionerClass(FieldIntPair.KeyPartitioner.class);
		
		job.setOutputFormatClass(FieldOutputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), FieldInputFormat.class, ExtractedPartsIOMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ExtractedL1RelnMapper.class);
		job.setReducerClass(Manu1Reducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0:1;
		
		
		
	}
	
	
	public static class Manu1Reducer extends Reducer<FieldIntPair, FieldIntPair, FieldWritable, NullWritable>{
		private FieldWritable keyOut =new FieldWritable(
				"part1" + "\t" + "part2" + "\t" +
				"weight" + "\t" + "isfff" + "\t" + "manu1"
				);
		
		@Override
		protected void reduce(FieldIntPair key, Iterable<FieldIntPair> vals,
				Reducer<FieldIntPair, FieldIntPair, FieldWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String defaultManu = "#####";
			HashSet<String> manuSet =  new HashSet<String>();
			Iterator<FieldIntPair> itr = vals.iterator();
			while (itr.hasNext()) {
				FieldIntPair fip = itr.next();
				if (fip.mark.get() == 0) {
					manuSet.add ( fip.field.get("manufacturer") );

				} else {
					if (manuSet.isEmpty() == false){
						for(String str: manuSet){
							keyOut.set(fip.field.toString() + "\t" + str);
							context.write(keyOut, NullWritable.get());
						}

					}else{
						keyOut.set(fip.field.toString() + "\t" + defaultManu);
						context.write(keyOut, NullWritable.get());
					}

				}

			}
		}
		
	}
	
	public static class ExtractedL1RelnMapper extends Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>{
		private FieldIntPair keyOut = new FieldIntPair(new FieldWritable("join_part"), 1);
		private FieldIntPair valOut = new FieldIntPair(new FieldWritable(
					"part1" + "\t" + "part2" + "\t" +
					"weight" + "\t" + "isfff" 
					),																	1);
				
				
		
		private static int thresholdWeight = 0;
		private static int whichPartIsKey  = -1;
		
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context)
				throws IOException, InterruptedException {
			
			String[] toks=value.toString().split("\t");
			int weight=Integer.parseInt(toks[2]);
			if(weight>thresholdWeight){
				context.getCounter("ExtractedL1RelnMapper", "Weight >" ).increment(1);
				return;
			}
			keyOut.field.set(toks[whichPartIsKey]);
			valOut.field.set(value);
			context.write(keyOut, valOut);
		}
		
		@Override
		protected void setup(Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context)
				throws IOException, InterruptedException {
			thresholdWeight = Integer.parseInt(context.getConfiguration().get(THRESHOLD_WEIGHT));
			whichPartIsKey = Integer.parseInt(context.getConfiguration().get(KEY_COL));
		}
		
	}
	
	
	public static class ExtractedPartsIOMapper extends Mapper<LongWritable, FieldWritable, FieldIntPair, FieldIntPair>{
		private FieldIntPair keyOut = new FieldIntPair(new FieldWritable("join_part"), 0);
		private FieldIntPair valOut = new FieldIntPair(new FieldWritable("manufacturer") , 0); 
		
		@Override
		protected void map(LongWritable key, FieldWritable value,
				Mapper<LongWritable, FieldWritable, FieldIntPair, FieldIntPair>.Context context)
				throws IOException, InterruptedException {

			String p=value.get("part_number");
			String m=value.get("manufacturer");
			
			keyOut.field.set(p);
			valOut.field.set(m);
			context.write(keyOut, valOut);
			
		}
	}
}
