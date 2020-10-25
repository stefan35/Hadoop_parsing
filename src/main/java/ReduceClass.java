import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text b = new Text();
        ArrayList<String> a = new ArrayList<String>();

        for (Text t : values) {
            //a.add(t.toString());
            context.write(key, t);
            }
        //b.set(String.valueOf(a));
        //context.write(key, b);

    }
}