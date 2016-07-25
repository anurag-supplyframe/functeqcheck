package functeqcheck;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar functeqcheck.Bz2ToGz  /user/amishra/related_parts_summary_2015_16_bzip2 /user/amishra/related_parts_summary_2015_16_gz

*/


public class Bz2ToGz extends Configured implements Tool {

	
	public static void main(String[] args) throws Exception {
		int rc=ToolRunner.run(new Configuration(), new Bz2ToGz(), args);
		System.exit(rc);
	}
	
	
	
	
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf= getConf();
		
		
		//compress the output into bzip2
	    conf.set("mapred.output.compress", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
	    conf.set("mapred.compress.map.output", "true");
	    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
	    conf.set("dfs.replication", "1");
	    conf.set("mapred.reduce.tasks","0");
	    
	    
	    Job job = new Job(conf);
	    
		job.setJarByClass(Bz2ToGz.class);
		job.setJobName("Bz2ToGz");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(Mapper.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    
	    
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	
	
	
	

	
	
	
}
