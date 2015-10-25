
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PunctuationReducerClass extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(LongWritable value1 : values) {
				sum += value1.get();
		}
		context.write(key, new LongWritable(sum));
		}
}
