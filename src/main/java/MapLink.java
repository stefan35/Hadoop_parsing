import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapLink extends Mapper<LongWritable, Text, Text, Text> {
    private Text link_id = new Text();
    private Text value = new Text();

    @Override
    public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
        String line = input_line.toString();

        Pattern link_pattern = Pattern.compile(".*name.*");
        Matcher link_matcher = link_pattern.matcher(line);

        if(!link_matcher.find()){
            String[] tmp_triplet = line.split("\t");

            link_id.set(tmp_triplet[0]);
            value.set(tmp_triplet[1]);
            context.write(link_id, value);
        }
    }
}
