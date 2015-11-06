package cloud.sea.crimecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context output)
			throws IOException, InterruptedException {
		int crimeCount = 0;
		for (IntWritable value : values) {
			crimeCount += value.get();
		}
		output.write(key, new IntWritable(crimeCount));
	}
}
