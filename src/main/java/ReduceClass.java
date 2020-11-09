import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {
    HashSet<String> all_links = new HashSet<String>();
    boolean new_link = false;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text value = new Text();
        HashSet<String> person = new HashSet<String>();
        ArrayList<String> link = new ArrayList<String>();
        boolean name = false;
        boolean link_flag = false;
        new_link = false;
        Pattern person_pattern = Pattern.compile("(date|gender|profession|alias|@en)");
        Matcher person_matcher;

        for (Text v : values) {
            person_matcher = person_pattern.matcher(v.toString());
            if(person_matcher.find() && !v.toString().contains("@en-"))
                person.add(v.toString());
            link.add(v.toString());
            if(v.toString().contains("name"))
                name = true;
            if(v.toString().contains("@"))//zmenit na charaktery
                link_flag = true;
            //System.out.println(v.toString() + " " + name + " " + link_flag);
        }

        if(name && person.size() > 2){
            /*Iterator k = person.iterator();
            while (k.hasNext()) {
                if(k.next().);
            }*/
            value.set(String.valueOf(person));
            context.write(key, value);
        }
        if(link_flag && !name){
            //System.out.println("B\n");
            for(int i = 0; i < link.size(); i++){
                if(!link.get(i).contains("\"")){
                    if(all_links.add(link.get(i))) {
                        new_link = true;
                        key.set(link.get(i));
                        link.remove(i);
                        i = -1;
                        //List <String> filtered = link.stream().filter(x -> x.indexOf("@en") != -1).collect(Collectors.toList());
                        /*if(filtered.size() > 1) {
                            for (int j = 0; j < filtered.size(); j++) {
                                if (filtered.get(j).contains("en-"))
                                    filtered.remove(j);
                            }
                        }
                        String[] split_value = filtered.get(0).split("\"");
                        filtered.add(split_value[1]);
                        filtered.remove(0);*/

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