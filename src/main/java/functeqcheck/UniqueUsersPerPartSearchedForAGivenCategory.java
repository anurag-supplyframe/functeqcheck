package functeqcheck;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FieldInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*

* 01. we will employ secondary sorting to get at the below query:
* 02. Be very careful while iteration in the reducer using the Text(or any hadoop) data type

select part ,count(distinct(user))  from /user/amishra/1year/related_parts_raw_15_16 
group by part 

This is same as running the below hive query
select 
innert.part_number , count(distinct(innert.user)) as cnt from
(
select 
part_number, concat(ct_remote_address,ct_user_agent) as user
from raw_1year
where category = "Microcontrollers and Processors"
) innert
group by innert.part_number

 */

/*
hadoop jar functeqcheck-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
functeqcheck.UniqueUsersPerPartSearchedForAGivenCategory \
-Dmapred.reduce.tasks=20 -Ddfs.replication=1 \
-Dcategory="Microcontrollers and Processors" \
/user/amishra/1year/related_parts_raw_15_16 \
/user/amishra/1year/part_user_search_group
 */


public class UniqueUsersPerPartSearchedForAGivenCategory extends Configured implements Tool{

	
	public static void main(String[] args) throws Exception {
		int rc= ToolRunner.run(new Configuration(), new UniqueUsersPerPartSearchedForAGivenCategory(), args);
		System.exit(rc);
	}
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf= getConf();
		
		
		Job job = new Job(conf);
		job.setJarByClass(UniqueUsersPerPartSearchedForAGivenCategory.class);
		job.setJobName("UniqueUsersPerPartSearchedForAGivenCategory");
		
		//job.setPartitionerClass(PartPartiioner.class);
		//we are modifying the hash function of the key. So the default hash partitioner can work OK
		job.setGroupingComparatorClass(PartGroupingComparator.class);
		
		job.setInputFormatClass(FieldInputFormat.class);
		FieldInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(UniqueUsersPerPartSearchedForAGivenCategoryMapper.class);
		job.setMapOutputKeyClass(PartAndUser.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setReducerClass(UniqueUsersPerPartSearchedForAGivenCategoryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		
		return job.waitForCompletion(true) ? 0:1;
		
		
		
	}
	
	public static class UniqueUsersPerPartSearchedForAGivenCategoryMapper extends Mapper<LongWritable, FieldWritable, PartAndUser, Text>{
		String cat = "";
		
		@Override
		protected void setup(Mapper<LongWritable, FieldWritable, PartAndUser, Text>.Context context)
				throws IOException, InterruptedException {
			cat=context.getConfiguration().get("category","");
			
		}
		
		@Override
		protected void map(LongWritable key, FieldWritable value,
				Mapper<LongWritable, FieldWritable, PartAndUser, Text>.Context context)
				throws IOException, InterruptedException {
			String part = value.get("part_number");
			String category = value.get("category");
			if("".equals(cat)){
				//no filter
			}else{
				if (! cat.equals(category)){
					//we have a category filter and it record is not of specified category
					return;
				}
			}
			String user = value.get("ct_remote_address") + "\t" + value.get("ct_user_agent");
			
			context.write(new PartAndUser(part, user), new Text(user));

		}
		
	}
	
	
	public static class UniqueUsersPerPartSearchedForAGivenCategoryReducer extends Reducer<PartAndUser, Text, Text, LongWritable>{
		
		
		@Override
		protected void reduce(PartAndUser key, Iterable<Text> vals,
				Reducer<PartAndUser, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			//our assumption is that the vals will be in ascending order
			//if the above assumption fails, we will throw an exception
			Iterator<Text> itr=vals.iterator();
			Text prev=new Text(), curr=null;
			long cnt=0;
			if(itr.hasNext()){
				curr=itr.next();
				prev.set(curr);
				
				++cnt;
			}
			while(itr.hasNext()){
				curr=itr.next();
				int cmp=curr.compareTo(prev);
				if(cmp < 0){
					StringBuilder msg = new StringBuilder("Logical error in secondary sorting:\n");
					msg.append(prev.toString()+" %%%%% "+curr.toString() + "\n");
					msg.append(cmp+"\n");
					
					throw new RuntimeException(msg.toString());
				}else if(cmp > 0){
					//there is a change
					++cnt;
					prev.set(curr);
				}else{
					//no change
				}
			}
			context.write(new Text(key.part), new LongWritable(cnt));

		}
	}
	
	
	
	
	public static class PartAndUser implements WritableComparable<PartAndUser>{

		Text part = new Text();
		Text user = new Text();
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((part == null) ? 0 : part.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PartAndUser other = (PartAndUser) obj;
			if (part == null) {
				if (other.part != null)
					return false;
			} else if (!part.equals(other.part))
				return false;
			return true;
		}

		public PartAndUser() {}

		public PartAndUser(String part, String user) {
			super();
			this.part.set(part);
			this.user.set(user);
		}


		@Override
		public void write(DataOutput out) throws IOException {
			part.write(out);
			user.write(out);
			
		}


		@Override
		public void readFields(DataInput in) throws IOException {
			part.readFields(in);
			user.readFields(in);
			
		}


		@Override
		public int compareTo(PartAndUser other) {
			
			int t = this.part.compareTo(other.part);
			if( t != 0){
				return t;
			}else{
				return this.user.compareTo(other.user);
			}
			
		}
		
		
		
	}
	
	/*
	 * we are using custom Partitioner so that same parts go the same node
	 * We are using key.part.hashCode() and not key.hashCode()
	 */
	public static class PartPartiioner extends Partitioner<PartAndUser, Text>{

		@Override
		public int getPartition(PartAndUser key, Text value, int numPartitions) {
			return (key.part.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
		
		
	}
	
	
	/*
	 * The group partitioner which makes sure that all the the items that have the same part are sent in one call 
	 * to the reducer
	 */
	public static class PartGroupingComparator extends WritableComparator{

		protected PartGroupingComparator() {
			super(PartAndUser.class,true);
			
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			PartAndUser o1=(PartAndUser)a;
			PartAndUser o2=(PartAndUser)b;
			
			return o1.part.compareTo(o2.part);
			
		}
		
	}
	
	
	
	

}
