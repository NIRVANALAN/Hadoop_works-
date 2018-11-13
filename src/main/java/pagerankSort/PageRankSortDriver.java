package pagerankSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class PageRankSortDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "PageRankSort");
//		job.setJarByClass(this.getClass());
        job.setJarByClass(PageText.class);

		Configuration conf = job.getConfiguration();

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		KeyValueTextInputFormat.addInputPath(job, input);
		TextOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(PageRankSortMapper.class);
		job.setMapOutputKeyClass(PageText.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(PageRankSortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 1 : 0;
	}

}
