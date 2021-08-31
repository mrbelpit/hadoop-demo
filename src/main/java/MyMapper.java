import java.io.IOException;
import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

  private static final IntWritable one = new IntWritable(1);

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {

    String line = value.toString();

    StringTokenizer tokenizer = new StringTokenizer(line);

    while (tokenizer.hasNext()) {
      String token = tokenizer.nextToken();
      if (token.equals(token.toUpperCase())) {
        Text characterName = new Text(token);
        output.collect(characterName, one);
      }
    }
  }
}
