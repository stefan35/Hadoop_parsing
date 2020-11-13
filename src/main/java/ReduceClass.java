import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {
    Text value = new Text();
    HashSet<String> all_links = new HashSet<String>();
    boolean new_link = false;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> person = new HashSet<String>();
        ArrayList<String> link = new ArrayList<String>();
        boolean name = false;
        boolean link_flag = false;
        new_link = false;
        Pattern person_pattern = Pattern.compile("(date|gender|profession|@en)");
        Matcher person_matcher;
        Pattern name_pattern = Pattern.compile("\"[A-Za-z ěščřžýáíéóúůôďťňĺľĽĎŇŤŠČŘŽÝÁÍÉÚĹ]+");
        Matcher name_matcher;

        for (Text v : values) {
            person_matcher = person_pattern.matcher(v.toString());
            name_matcher = name_pattern.matcher(v.toString());

            if((name_matcher.find() && v.toString().contains("alias")) || (person_matcher.find() && !v.toString().contains("@en-") && !v.toString().contains("alias")))
                person.add(v.toString());
            link.add(v.toString());
            if(v.toString().contains("name"))
                name = true;
            if(v.toString().contains("@"))
                link_flag = true;
        }

        if(name && person.size() > 2){
            value.set(String.valueOf(person));
            context.write(key, value);
        }

        if(link_flag && !name){
            for(int i = 0; i < link.size(); i++){
                if(!link.get(i).contains("\"")){
                    if(all_links.add(link.get(i))) {
                        new_link = true;
                        key.set(link.get(i));
                        link.remove(i);
                        i = -1;
                    }
                }
                else if(!link.get(i).contains("@en") || link.get(i).contains("@en-")){
                    link.remove(i);
                    i = -1;
                }
            }

            if(new_link && link.size() > 0) {
                String[] split_value = link.get(0).split("\"");
                ArrayList<String> tmp = new ArrayList<String>();
                tmp.add(split_value[1]);

                value.set(String.valueOf(tmp));
                context.write(key, value);
            }
        }
    }
}