import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ReduceLink extends Reducer<Text, Text, Text, Text> {
    HashSet<String> all_links = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text value = new Text();
        ArrayList<String> link = new ArrayList<String>();
        String a = "";

        for (Text v : values) {
            link.add(v.toString());
        }
        a = String.join(",", link);

        value.set(a);
        context.write(key, value);
        //if(link.size() == 1)
            //return;

        /*for(int i = 0; i < link.size(); i++){
            if(!link.get(i).contains("\"")){
                if(all_links.add(link.get(i))) {
                    key.set(link.get(i));
                    link.remove(i);
                    List<String> filtered = link.stream().filter(x -> x.indexOf("@en") != -1).collect(Collectors.toList());
                    if(filtered.size() > 1) {
                        for (int j = 0; j < filtered.size(); j++) {
                            if (filtered.get(j).contains("en-"))
                                filtered.remove(j);
                        }
                    }
                    value.set(String.valueOf(filtered));
                    context.write(key, value);
                }
                break;
            }
        }*/
    }
}
