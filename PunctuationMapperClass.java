

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PunctuationMapperClass extends Mapper<LongWritable, Text, Text, LongWritable>{

	Text commaText = new Text("Comma's count");
	Text semicolonText = new Text("Semicolon's count");
	Text periodText = new Text("Period's count");
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String lineValue = value.toString();
		if(lineValue.contains(",")) {
			context.write(commaText, new LongWritable(countPunct(lineValue,',')));
		}
		if(lineValue.contains(".")) {
			context.write(periodText, new LongWritable(countPunct(lineValue,'.')));
		}
		if(lineValue.contains(",")) {
			context.write(semicolonText, new LongWritable(countPunct(lineValue,';')));
		}
		
	}
	
	/**
	 * Method to find the occurrences of give character
	 * @param line
	 * @param punctChar
	 * @return
	 */
	private int countPunct(String line, char punctChar) {
		int charCount = 0;
		int numOfPunchar=0;
		while(charCount < line.length()) {
			
			char myChar = line.charAt(charCount);
			if(myChar == punctChar) {
				numOfPunchar++;
			}
			charCount++;
			
		}
		return numOfPunchar;
	}
}
