package imageCounter;
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


public class ImageCounter {
	
	
	public static class ImageCounterMapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private static final int MISSING = 9999;
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String line = value.toString();
		  boolean getDetected = line.contains("GET");
		  boolean jpgDetected = line.contains(".jpg");
		  boolean gifDetected = line.contains(".gif");
		  boolean pngDetected = line.contains(".png");
		  if(jpgDetected && getDetected) {
			  Text opkey = new Text("jpeg");
			  IntWritable opvalue = new IntWritable(1);
			  context.write(opkey,opvalue);
		  } else if(gifDetected && getDetected) {
			  Text opkey = new Text("gif");
			  IntWritable opvalue = new IntWritable(1);
			  context.write(opkey,opvalue);
		  } else if(pngDetected && getDetected){
			  Text opkey = new Text("others");
			  IntWritable opvalue = new IntWritable(1);
			  context.write(opkey,opvalue);
		  }
		
	  }
	}
	
	public static class ImageCounterReducer
	  extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
	      Context context)
	      throws IOException, InterruptedException {
	    
	    int total = 0;
	    for (IntWritable value : values) {
	      //maxValue = Math.max(maxValue, value.get());
	    	total+=value.get();
	    }
	    context.write(key, new IntWritable(total));
	  }
	}
	
	// main
	public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println("Usage: ImageCounter <input path> <output path>");
	      System.exit(-1);
	    }
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"ImageCounter") ;
	    job.setJarByClass(ImageCounter.class);
	    job.setMapperClass(ImageCounterMapper.class);
	    job.setReducerClass(ImageCounterReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path outputPath = new Path(args[1]);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setJobName("Image Counter");

	    
	    
	    

	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
