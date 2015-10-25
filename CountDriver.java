
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Punctuation Count Driver
 *
 */
public class CountDriver extends Configured implements Tool
{
    public static void main( String[] args ) {
    	try {
			int exitCode = ToolRunner.run(new CountDriver(),args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	public int run(String[] args) throws Exception {
		if(args.length !=2) {
			System.out.printf("Two parameters are required -<input dir> <output dir> \n");
		}
		Job job = new Job();
		
		job.setJarByClass(CountDriver.class);
		job.setMapperClass(PunctuationMapperClass.class);
		job.setReducerClass(PunctuationReducerClass.class);
		job.setCombinerClass(PunctuationReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		return (status == true) ? 0 : 1;
	} 
}
