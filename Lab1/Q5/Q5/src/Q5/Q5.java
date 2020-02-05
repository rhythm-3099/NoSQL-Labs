package Q5;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class Q5 {
	
	
	/*public static class Q5Mapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private static final int MISSING = 9999;
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    
	    String line = value.toString();
	    if(line.contains(" 400 ")) {
	    	int n=line.length();
	    	String website="";
	    	String timestamp="";
	    	for(int i=0;i<n-4;i++) {
	    		if(line.charAt(i)=='[') {
	    			int j=i+1;
	    			while(line.charAt(j)!=']') {
	    				timestamp+=line.charAt(j);
	    				j++;
	    			}
	    			break;
	    		}
	    	}
	    	for(int i=0;i<n-4;i++) {
	    		if(line.charAt(i)=='h' && line.charAt(i+1)=='t' && line.charAt(i+2)=='t' && line.charAt(i+3)=='p') {
	    			website+=line.charAt(i);
	    			website+=line.charAt(i+1);
	    			website+=line.charAt(i+2);
	    			website+=line.charAt(i+3);
	    			int j=i+4;
	    			while(line.charAt(j)!='"') {
	    				website+=line.charAt(j);
	    				j++;
	    			}
	    			break;
	    		}
	    	}
	    	//website+=" ";
	    //	website+=timestamp;
	    	website = "rhythm";
	    	Text websiteURL = new Text(website);
	    	//Text timestampInfo = new Text(timestamp);
	    	//context.write(websiteURL, timestampInfo);
	    	context.write(websiteURL, new IntWritable(1));
	    }
	  }
	}*/
	
	public static class Q5Mapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private static final int MISSING = 9999;
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String line = value.toString();
		    if(line.contains(" 404 ")) {
		    	int n=line.length();
		    	String website="";
		    	String timestamp="";
		    	for(int i=0;i<n-4;i++) {
		    		if(line.charAt(i)=='[') {
		    			int j=i+1;
		    			while(line.charAt(j)!=']') {
		    				timestamp+=line.charAt(j);
		    				j++;
		    			}
		    			break;
		    		}
		    	}
		    	for(int i=0;i<n-4;i++) {
		    		if(line.charAt(i)=='h' && line.charAt(i+1)=='t' && line.charAt(i+2)=='t' && line.charAt(i+3)=='p') {
		    			website+=line.charAt(i);
		    			website+=line.charAt(i+1);
		    			website+=line.charAt(i+2);
		    			website+=line.charAt(i+3);
		    			int j=i+4;
		    			while(line.charAt(j)!='"') {
		    				website+=line.charAt(j);
		    				j++;
		    			}
		    			break;
		    		}
		    	}
		    	website+=" ";
		    	website+=timestamp;
		    	//website = "rhythm";
		    	Text websiteURL = new Text(website);
		    	String dumm = " ";
		    	dumm+=timestamp;
		    	if(website.equals(dumm)==false) {
		    	context.write(websiteURL, new IntWritable(404));
		    	}
		    }
		
	  }
	}
	
	public static class Q5Reducer
	  extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
	      Context context)
	      throws IOException, InterruptedException {
	    
	    
	    
	  }
	}
	
	// main
	public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println("Usage: MaxTemperature <input path> <output path>");
	      System.exit(-1);
	    }
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"Q5") ;
	    job.setJarByClass(Q5.class);
	    job.setMapperClass(Q5Mapper.class);
	    //job.setReducerClass(Q5Reducer.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path outputPath = new Path(args[1]);
	    
	 // Sets reducer tasks to 0
	    
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setJobName("404ErrorJob");

	    
	    
	    

	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
