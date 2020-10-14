import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputPath = "part-r-00000";
		String outputPath = "PFout";

		//iteration 75 times
		for (int i = 1; i < 75; i++) {
			Job job = new Job(conf, "PageRank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(mapper.class);
			job.setReducerClass(reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			//Change the output address to the input address of the next iteration
			inputPath = outputPath;
			// Set the next output to a new address
			outputPath = outputPath + i;
			job.waitForCompletion(true);
		}
	}

	public static class mapper extends Mapper<Object, Text, Text, Text> {
		private String id;
		private float pr;
		private int count;
		private float avgpr;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// split value
			StringTokenizer token = new StringTokenizer(value.toString());
			// id is the first word parsed, representing the current webpage
			id = token.nextToken();
			// pr is the second word parsed, converted to float type, representing PageRank value
			pr = Float.parseFloat(token.nextToken());
			// count is the number of remaining words, representing the number of outgoing pages of the current page
			count = token.countTokens();
			// Find the contribution value of the current webpage to the outgoing webpage
			avgpr = pr / count;

			// The following are the two types of output from map, respectively distinguished by 'P' and 'N'
			String addresses = "N";
			while (token.hasMoreTokens()) {
				String address = token.nextToken();
				// The output is <out-link webpage, contribution value obtained>
				context.write(new Text(address), new Text("P" + avgpr));
				addresses += " " + address;
			}
			// The output is <current webpage, all outgoing webpages>
			context.write(new Text(id), new Text(addresses));
		}
	}

	public static class reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String link = "";
			float pr = 0;
            /* Analyze each value in values ​​and judge whether the first character is 'P' or 'N'
			Through this cycle, you can find the sum of the contribution values ​​obtained by the current page, that is, the new PageRank value; at the same time find the current
			All outgoing pages of the webpage */
			for (Text value : values) {
				if (value.toString().substring(0, 1).equals("P")) {
					pr += Float.parseFloat(value.toString().substring(1));
				} else if (value.toString().substring(0, 1).equals("N")) {
					link += value.toString().substring(1);
				}
			}

			//add parameter beta = 0.85 to prevent the spider trap
			//There are only 600493 nodes connected
			pr = 0.85f * pr + 0.15f * (1.0f / 600493);
			String result = pr + link;
			context.write(key, new Text(result));
		}
	}
}

