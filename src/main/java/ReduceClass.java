import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> final_attributes = new ArrayList<String>();
        Text assign = new Text();
        String line = key.toString();
        line = line.replaceAll("[\\[\\]]", "");
        String[] attributes = line.split(",");
        //System.out.println(attributes[0]);


        for(Text a : values){
            for(int i = 0; i < attributes.length; i++){
                String[] tmp;
                String reverse;
                if(!attributes[i].contains("\"")) {
                    tmp = attributes[i].split("\\.");
                    if(tmp.length == 5) {
                        tmp[4] = tmp[4].substring(0, tmp[4].length() - 1);
                        final_attributes.add(tmp[4]);
                    }
                    else {
                        tmp[3] = tmp[3].substring(0, tmp[3].length() - 1);
                        tmp[2] = tmp[2].substring(tmp[2].length() - 1, tmp[2].length());
                        final_attributes.add(tmp[2] + "." + tmp[3]);
                    }
                }else if(!attributes[i].contains("@")){
                    tmp = attributes[i].split("\"");
                    final_attributes.add(tmp[1]);
                }
                else{
                    final_attributes.add(attributes[i]);
                }
            }

            final_attributes = checkName(final_attributes);



            BufferedReader reader = new BufferedReader(new FileReader("values.txt"));
            String line_read;
            String[] tmp_file;
            while ((line_read = reader.readLine()) != null) {
                tmp_file = line_read.split("\\+");
                for(int j = 0; j < final_attributes.size(); j++){
                    System.out.println(final_attributes.get(j) + " " + tmp_file[1]);
                    if(final_attributes.get(j).equals(tmp_file[1])){
                        tmp_file[0] = tmp_file[0].substring(1, tmp_file[0].length()-1);
                        final_attributes.set(j, tmp_file[0]);
                    }
                }
            }
            reader.close();

            assign.set(String.valueOf(final_attributes));
            context.write(assign, a);
        }
    }

    public ArrayList<String> checkName(ArrayList<String> list){
        ArrayList<String> a = new ArrayList<String>();
        String[] language;
        boolean find = false;

        for(int i = 0; i < list.size(); i++){
            if(list.get(i).contains("name")){
                language = list.get(i+1).split("@");
                for(int j = 0; j < a.size(); j++){
                    if(a.get(j).equals(language[1])){
                        find = true;
                        list.remove(i+1);
                        list.remove(i);
                        i = i - 2;
                        break;
                    }
                }
                if(find == true)
                    find = false;
                else if(find == false)
                    a.add(language[1]);
            }
        }
        //System.out.println(a);
        return list;
    }
}