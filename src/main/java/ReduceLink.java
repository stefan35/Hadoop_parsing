import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class ReduceLink extends Reducer<Text, Text, Text, Text> {
    HashSet<String> all_links = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text b = new Text();
        ArrayList<String> a = new ArrayList<String>();

        for (Text t : values) {
            a.add(t.toString());
        }

        if(a.size() == 0)
            return;

        for(int i = 0; i < a.size(); i++){
            if(!a.get(i).contains("\"")){
                if(all_links.add(a.get(i))) {
                    key.set(a.get(i));
                    a.remove(i);
                    b.set(String.valueOf(a));
                    context.write(key, b);
                }
                break;
            }
        }
    }
}
