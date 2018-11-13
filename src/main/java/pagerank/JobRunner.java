package pagerank;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import pagerankSort.PageRankSortDriver;


public class JobRunner {
    private static final Log log = LogFactory.getLog(JobRunner.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out
                    .println("please just input one parameter for appoint how many times to count pagerank, such as 10.");
            System.exit(0);
        } // check if the input is enough
        int totalCounts = Integer.parseInt(args[0]);//
        int result = 0;

        String input_dir = "input_page_rank/";
        String output_dir = "output_page_rank/";

        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        if (fs.exists(new Path(output_dir))) {
//            fs.delete(new Path(output_dir), true);
//        }
        String temp = null;
        PageRankDriver driver;
        for (int i = 0; i < totalCounts; i++) {
            driver = new PageRankDriver();
            driver.setConf(conf);
//            result = ToolRunner.run(driver, new String[]{String.valueOf(i),
//                    input_dir, output_dir});
            result = ToolRunner.run(driver, new String[]{String.valueOf(i), input_dir, output_dir});
            if (result > 0) {
                temp = input_dir;
                input_dir = output_dir;
                output_dir = temp;
                log.info("job_" + i + " is success ...");
            } else {
                log.info("job_" + i + " is failed ...");
            }
        }
        { // pagerank sort
            output_dir = "output_page_rank_sorted/";

            conf = new Configuration();
            temp = null;
            PageRankSortDriver sortDriver;
            sortDriver = new PageRankSortDriver();
            sortDriver.setConf(conf);
//            result = ToolRunner.run(driver, new String[]{String.valueOf(i),
//                    input_dir, output_dir});
            result = ToolRunner.run(sortDriver, new String[]{input_dir, output_dir});
            if (result > 0) {
                log.info("sort is success ...");
            } else {
                log.info("sort is failed ...");
            }
        }

    }
}
