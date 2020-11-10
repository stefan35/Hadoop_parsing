import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ReduceLink extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text value = new Text();
        ArrayList<String> link = new ArrayList<String>();
        String join_link = "";

        for (Text v : values) {
            link.add(v.toString());
        }
        join_link = String.join(",", link);

        value.set(join_link);
        context.write(key, value);
    }
}
