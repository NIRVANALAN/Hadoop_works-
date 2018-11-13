package pagerank;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	private static final Log log = LogFactory.getLog(PageRankMapper.class);

    private static float factor = 0.85f;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        factor = context.getConfiguration().getFloat("mapred.pagerank.factor",
                0.85f);
    }

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        log.info(key.toString() + ";" + value.toString());
        if (value.toString().length() == 0) {
            return;
        }
        String[] outLinks = value.toString().split(",");
        String[] link = key.toString().split(",");
        float rank = factor;
        if (link.length > 1) {
            rank = Float.parseFloat(link[1]);
        }
        int outLinkLen = outLinks.length;
//		for (String s : outLinks) {
//			context.write(new Text(s), new Text(link[0] + ";" + rank + ";"
//					+ outLinkLen));
//		}
		for (String s:outLinks
			 ) {
			context.write(new Text(s), new Text(link[0]+';'+rank+';'+outLinkLen));
		}
		context.write(new Text(link[0]), value);  // for later input
	}
}
