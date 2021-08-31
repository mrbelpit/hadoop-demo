import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {
  public static void main(String[] args) throws IOException {
    JobConf configuration = new JobConf(Main.class);
    configuration.setJobName("Finding the chattiest character with direct connection to hadoop");

    FileInputFormat.setInputPaths(configuration, new Path("hdfs://localhost:9000/user/root/input"));
    FileOutputFormat.setOutputPath(configuration,  new Path("hdfs://localhost:9000/user/root/result3"));

    configuration.setOutputKeyClass(Text.class);
    configuration.setOutputValueClass(IntWritable.class);

    configuration.setMapperClass(MyMapper.class);
    configuration.setCombinerClass(MyReducer.class);
    configuration.setReducerClass(MyReducer.class);

    JobClient.runJob(configuration);
  }
}
