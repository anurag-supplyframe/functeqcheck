package functeqcheck;

//Command to run: 
/*
hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
functeqcheck.PartIOExtract \
-Dmapred.reduce.tasks=0 -Ddfs.replication=1 \
/user/amishra/partsio_extract
*/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FieldOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartIOExtract extends Configured implements Tool {

	private static String partsio_path = "/prod/partsio/indexFile";
	
	
	
	public static void main(String[] args) throws Exception {
		int rc=ToolRunner.run(new Configuration(), new PartIOExtract(), args);
		System.exit(rc);
	}
	@Override
	public int run(String[] args) throws Exception {
		
		
		
		Configuration conf = getConf();  
		
		
		conf.set("mapred.output.compress", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
	    conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.GzipCodec");
		conf.set("mapred.reduce.tasks", "0");
		
		Job  job = new Job(conf);
		job.setJarByClass(PartIOExtract.class);
		job.setJobName("PartIOExtract");
		
		job.setOutputKeyClass(FieldWritable.class);
		job.setOutputValueClass(FieldWritable.class);
		job.setMapOutputKeyClass(FieldWritable.class);
		job.setMapOutputValueClass(FieldWritable.class);
		job.setMapperClass(PartIOExtractMapper.class);
		job.setReducerClass(Reducer.class);//we will not be using reducer
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(FieldOutputFormat.class);//this is important
		
		
		FileInputFormat.setInputPaths(job, new Path(partsio_path));
		FieldOutputFormat.setOutputPath(job, new Path(args[0]));//can be field or file
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	
	public static class PartIOExtractMapper extends Mapper<LongWritable, Text, FieldWritable, NullWritable>{
		
		private FieldWritable keyOut = new FieldWritable("part_number" + "\t" + "category" + "\t" + "manufacturer");
		
		
		Integer part_loc = 0, category_loc = 0;
		
		Integer IhsManufacturer = 0;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{

			
			System.out.println("PartMapper\n");
			
			Configuration conf = context.getConfiguration();
			
			// read the first record of partsio file to verify the column location 
			Path partsio_list = new Path(partsio_path + "/part-r-00000.gz");
			
			FileSystem fs = FileSystem.get(conf);
			InputStream gzipStream = new GZIPInputStream(fs.open(partsio_list));
			Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
			BufferedReader br = new BufferedReader(decoder);

			String line;
			String[] parts;
			if ((line = br.readLine()) != null){
				
	    		parts = line.toString().split("\t");
		    	
				// read the header line and find the part number and category fields
				for (int i = 0; i < parts.length; i++) {
					//System.out.println("field:\t" + i +"\t**"+ parts[i] + "**");
					if (parts[i].equals("MfrPartNumber")) {
						part_loc = i;
						System.out.println("found part:\t"+i);
					}
					if (parts[i].equals("IhsClass")) {
						category_loc = i;
						System.out.println("found category:\t"+i);
					}
					if(parts[i].equals("Manufacturer")){
						IhsManufacturer=i;
						System.out.println("found the manufacturer:\t"+i);
					}
				}
				
			}
			// reading just the 1st line
			gzipStream.close();

		}
		
		@Override
		public void map(LongWritable keyIn, Text valIn, Context context) throws IOException, InterruptedException {

			String[] parts = valIn.toString().split("\t");
				
			String conform_number = parts[part_loc].replaceAll("[\\s-]", "").replaceAll("</?[^>]+>", "").replace("\"", "");
			String category = parts[category_loc].replace("\"", "");
			if("".equals(category) ){
				category = "#####";
			}
			String manufacturer = parts[IhsManufacturer].replace("\"", "");
			if("".equals(manufacturer) ){
				manufacturer = "#####";
			}
			keyOut.set(conform_number.toUpperCase().trim() + "\t" +
					category+ "\t" +
					manufacturer
					);
			
			context.write(keyOut, NullWritable.get());
			
		}
	}

}
