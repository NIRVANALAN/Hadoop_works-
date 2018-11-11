package pagerank;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class JobRunner {
    private static final Log log = LogFactory.getLog(JobRunner.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out
                    .println("please just input one parameter for appoint how many times to count pagerank, such as 10.");
            System.exit(0);
        }
        int totalCounts = Integer.parseInt(args[0]);//
        int result = 0;

        String input_dir = "input_page_rank/";
        String output_dir = "output_page_rank/";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(output_dir))) {
            fileSystem.delete(new Path(output_dir));
        }
        String temp = null;
        PageRankDriver driver;
        for (int i = 0; i < totalCounts; i++) {
            driver = new PageRankDriver();
            driver.setConf(conf);
            result = ToolRunner.run(driver, new String[]{String.valueOf(i),
                    input_dir, output_dir});
            if (result > 0) {
                temp = input_dir;
                input_dir = output_dir;
                output_dir = temp;
                log.info("job_" + i + " is success ...");
            } else {
                log.info("job_" + i + " is failed ...");
            }
        }
    }
}
