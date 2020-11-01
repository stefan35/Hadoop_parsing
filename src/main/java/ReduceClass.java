import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text b = new Text();
        HashSet<String> a = new HashSet<String>();

        for (Text t : values) {
            a.add(t.toString());
        }

        Iterator<String> i = a.iterator();
        while (i.hasNext()){
            if(i.next().matches("name.*")){
                b.set(String.valueOf(a));
                context.write(key, b);
                break;
            }
        }
        //roznorode mena
    }
}