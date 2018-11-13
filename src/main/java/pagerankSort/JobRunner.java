package pagerankSort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class JobRunner {
    private static final Log log = LogFactory.getLog(JobRunner.class);

    public static void main(String[] args) throws Exception {
        int result;
        String input_dir = "output_page_rank/";
        String output_dir = "output_page_rank_sorted/";

        Configuration conf = new Configuration();
        String temp = null;
        PageRankSortDriver driver;
        driver = new PageRankSortDriver();
        driver.setConf(conf);
//            result = ToolRunner.run(driver, new String[]{String.valueOf(i),
//                    input_dir, output_dir});
        result = ToolRunner.run(driver, new String[]{input_dir, output_dir});
        if (result > 0) {
            log.info("sort is success ...");
        } else {
            log.info("sort is failed ...");
        }
    }
}
//}
