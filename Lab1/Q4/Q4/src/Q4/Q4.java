package Q4;
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


/*for(int i=0;i<n;i++) {
if(line.charAt(i)=='[') {
	  int j=i+1;
	  while(line.charAt(j)!=':') {
		  date+=line.charAt(j);
		  j++;
	  }
} 
if(line.charAt(i)=='"' && count==0) {
	  count++;
} else if(line.charAt(i)=='"' && count==1) {
	  count=2;
	  ind=i;
	  break;
}
}*/


public class Q4 {
	
	public static int pubj; 
	public static class Q4Mapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private static final int MISSING = 9999;
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String str = value.toString();

		  int i=0;
		  while(i<str.length() && str.charAt(i)!='[')
		  {
			  i++;
		  }
		  i+=4;
		  
		  String month=String.valueOf(str.charAt(i))+String.valueOf(str.charAt(i+1))+String.valueOf(str.charAt(i+2));
		  i+=4;
		  
		  String year=String.valueOf(str.charAt(i))+String.valueOf(str.charAt(i+1))+String.valueOf(str.charAt(i+2))+String.valueOf(str.charAt(i+3));

		  value.set(month+"-"+year);
		  
		  while(i<str.length() && str.charAt(i)!='"')
		  {
			  i++;
		  }
		  i++;
		  
		  while(i<str.length() && str.charAt(i)!='"')
		  {
			  i++;
		  }

		  i++;

		  while(i<str.length() && str.charAt(i)!=' ')
		  {
			  i++;
		  }

		  i+=2;

		  while(i<str.length() && str.charAt(i)!=' ')
		  {
			  i++;
		  }

		  String sizestr="";
		  int dataDownloaded=0;
		  i++;

		  if(str.charAt(i)!='-')
		  {
			  while(i<str.length() && str.charAt(i)!=' ')
			  {
				  sizestr+=str.charAt(i);
				  i++;
			  }
			  dataDownloaded=Integer.parseInt(sizestr);
		  }

		  context.write(value ,new IntWritable(dataDownloaded));
		  
	  }
	}
	
	public static class Q4Reducer
	  extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
	      Context context)
	      throws IOException, InterruptedException {
	    
		  int sum=0;
		  int total=0;
		  for(IntWritable x: values)
		  {
			  sum+=x.get();
			  total++;
		  }

		  String scnt=Integer.toString(total);

		  String nkey = key.toString();
		  key.set(nkey+" "+scnt);
		  context.write(key, new IntWritable(sum));
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
	    job.setJarByClass(Q4.class);
	    job.setMapperClass(Q4Mapper.class);
	    job.setReducerClass(Q4Reducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path outputPath = new Path(args[1]);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setJobName("Monthly inventory");

	    
	    
	    

	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
