import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text value = new Text();
        HashSet<String> person = new HashSet<String>();

        for (Text v : values) {
            person.add(v.toString());
        }

        Iterator<String> i = person.iterator();
        while (i.hasNext()){
            if(i.next().matches("name.*")){
                value.set(String.valueOf(person));
                context.write(key, value);
                break;
            }
        }
    }
}