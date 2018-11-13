package pagerankSort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankSortMapper extends Mapper<Text, Text, PageText, Text> {
	private static final Log log = LogFactory.getLog(PageRankSortMapper.class);
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        log.info(key.toString() + ";" + value.toString());
        String[] link = key.toString().split(",");
        PageText k2 = new PageText(link[0],Float.valueOf(link[1]),value.toString().split("'").length);
        context.write(k2,value);
	}
}
