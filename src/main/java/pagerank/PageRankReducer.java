package pagerank;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	private static final Log log = LogFactory.getLog(PageRankReducer.class);

    private static float factor = 0.85f;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        // factor default value 0.85f
        factor = context.getConfiguration().getFloat("mapred.pagerank.factor",
                0.85f);
    }

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		log.info(key.toString());
		float rank = 1 - factor;// PageRankֵ
		String[] str;
		Text outLinks = new Text();
		for (Text t : values) {
			str = t.toString().split(";");
			if (str.length == 3) {
				rank += Float.parseFloat(str[1]) / Integer.parseInt(str[2])
						* factor; // page rank formula
			} else {
				outLinks.set(t.toString());
			}
		}
		context.write(new Text(key.toString() + "," + rank), outLinks);
	}
}
