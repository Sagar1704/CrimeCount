package cloud.sea.crimecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text regionCrime = new Text();

	@Override
	public void map(Object key, Text value, Context output) throws IOException, InterruptedException {
		String crimeLine = value.toString();

		String[] crimeData = crimeLine.split(",");
		String easting = "";
		if (crimeData.length > 4)
			easting = getRegion(crimeData[4]);

		String northing = "";
		if (crimeData.length > 5)
			northing = getRegion(crimeData[5]);

		String crime = "";
		if (crimeData.length > 7) {
			crime = crimeData[7];

			regionCrime.set(easting + "," + northing + "," + crime);

			output.write(regionCrime, one);
		}
	}

	private String getRegion(String region) {
		String temp = "";

		if (region != null && !region.isEmpty()) {
			temp = region.substring(0, 1);

			for (int i = 0; i < region.length() - 1; i++)
				temp += "x";
		}
		return temp;
	}
}
