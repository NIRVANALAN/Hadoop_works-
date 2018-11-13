package pagerankSort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankSortReducer extends Reducer<PageText, Text, Text, Text> {
	private static final Log log = LogFactory.getLog(PageRankSortReducer.class);
	@Override
	protected void reduce(PageText key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		log.info(key.toString());
		String[] str;
		Text outLinks = new Text();
		for (Text t : values) {
				outLinks.set(t.toString());
		}
		if (outLinks.getLength()==0)
		    outLinks.set("no outLinks");
		context.write(new Text(key.toString()), outLinks);
	}
}
