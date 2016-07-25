package functeqcheck;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
hadoop jar arbitaryutils-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
arbitaryutils.ExtractData \
-Dmapred.reduce.tasks=10 -Ddfs.replication=1 \
/user/amishra/L1reln /user/amishra/L1reln_extracted
 * 
 */

public class ExtractData extends Configured implements Tool{
	
	
	public static void main(String[] args) throws Exception {
		int rc=ToolRunner.run(new Configuration(), new ExtractData(), args);
		System.exit(rc);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		Configuration conf= getConf();
		
		Job job=new Job(conf);
		job.setJarByClass(ExtractData.class);
		job.setJobName("ExtractData");
		
		
		job.setMapperClass(ExtractMapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
		
	public static class ExtractMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		private Text out= new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] toks=value.toString().split("\t");
			out.set(toks[0] + "\t" + toks[1] + "\t" + toks[2] + "\t"  + toks[5]);
			context.write(out, NullWritable.get());
			
		}
	}

}
