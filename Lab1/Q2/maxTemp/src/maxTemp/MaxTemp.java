package maxTemp;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class MaxTemp {
	
	
	public static class MaxTemperatureMapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private static final int MISSING = 9999;
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    
	    String line = value.toString();
	    String year = line.substring(15, 19);
	    int airTemperature;
	    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
	      airTemperature = Integer.parseInt(line.substring(88, 92));
	    } else {
	      airTemperature = Integer.parseInt(line.substring(87, 92));
	    }
	    String quality = line.substring(92, 93);
	    if (airTemperature != MISSING && quality.matches("[01459]")) {
	      context.write(new Text(year), new IntWritable(airTemperature));
	    }
	  }
	}
	
	public static class MaxTemperatureReducer
	  extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
	      Context context)
	      throws IOException, InterruptedException {
	    
	    int maxValue = Integer.MIN_VALUE;
	    for (IntWritable value : values) {
	      maxValue = Math.max(maxValue, value.get());
	    }
	    context.write(key, new IntWritable(maxValue));
	  }
	}
	
	// main
	public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println("Usage: MaxTemperature <input path> <output path>");
	      System.exit(-1);
	    }
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"MaxTemp") ;
	    job.setJarByClass(MaxTemp.class);
	    job.setMapperClass(MaxTemperatureMapper.class);
	    job.setReducerClass(MaxTemperatureReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path outputPath = new Path(args[1]);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setJobName("Max temperature");

	    
	    
	    

	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
