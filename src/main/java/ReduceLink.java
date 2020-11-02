import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class ReduceLink extends Reducer<Text, Text, Text, Text> {
    HashSet<String> all_links = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text value = new Text();
        ArrayList<String> link = new ArrayList<String>();

        for (Text v : values) {
            link.add(v.toString());
        }
        System.out.println("a");

        if(link.size() == 1)
            return;

        for(int i = 0; i < link.size(); i++){
            if(!link.get(i).contains("\"")){
                if(all_links.add(link.get(i))) {
                    key.set(link.get(i));
                    link.remove(i);
                    value.set(String.valueOf(link));
                    context.write(key, value);
                }
                break;
            }
        }
    }
}
