/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Deepak
 */
public class kreducer extends Reducer<Text, Text, Text, Text> {

    Text word = new Text();
    public static int iter = 0;

    public static ArrayList<Double> calculateCentroids(List<Gene> input) {
        ArrayList<Double> avgList = new ArrayList<>();
        double avg, sum;
        for (int i = 0; i < input.get(0).getExpression().size(); i++) {
            sum = 0;
            avg = 0;
            for (int j = 0; j < input.size(); j++) {
                sum += input.get(j).getExpression().get(i);
            }
            avg = sum / input.size();
            avgList.add(avg);
        }
        return avgList;
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (iter < 9) {
            Configuration jc = context.getConfiguration();
            String[] cexpstr = new String[5];
            cexpstr[0] = jc.get("1");
            cexpstr[1] = jc.get("2");
            cexpstr[2] = jc.get("3");
            cexpstr[3] = jc.get("4");
            cexpstr[4] = jc.get("5");
            Map<Integer, List<Double>> cmap = new HashMap<Integer, List<Double>>();//cluster id with exp values
            for (int i = 0; i < 5; i++) {
                Integer cid = i + 1;
                String[] temp = cexpstr[i].split(" ");
                List<Double> exptemp = new ArrayList<Double>();
                for (int k = 0; k < temp.length; k++) {
                    exptemp.add(Double.parseDouble(temp[k]));
                }
                cmap.put(cid, exptemp);
            }
            int gcount = Integer.parseInt(jc.get("genecount"));
            Map<Integer, Gene> totalgenes = new HashMap<Integer, Gene>();//list of all genes with exp values
            for (int i = 1; i <= gcount; i++) {
                Integer geneid = i;
                String expstr = jc.get("g" + i);
                String[] est = expstr.split(" ");
                List<Double> exptemp = new ArrayList<Double>();
                for (int k = 0; k < est.length; k++) {
                    exptemp.add(Double.parseDouble(est[k]));
                }
                Gene g = new Gene(geneid, exptemp);
                totalgenes.put(geneid, g);
            }
            Iterator itr = values.iterator();
            Map<Integer, List<Gene>> clustered = new HashMap<Integer, List<Gene>>();
            while (itr.hasNext()) {
                Text temp = (Text) itr.next();
                String[] tmp = temp.toString().split(":");
                Integer igid = Integer.valueOf(tmp[0]);
                Integer icid = Integer.valueOf(tmp[1]);
                Gene g = totalgenes.get(igid);
                if (clustered.containsKey(icid)) {
                    List<Gene> t = clustered.get(icid);
                    t.add(g);
                    clustered.put(icid, t);
                } else {
                    List<Gene> t = new ArrayList<Gene>();
                    t.add(g);
                    clustered.put(icid, t);
                }

            }
            StringBuilder out=new StringBuilder();
            for (int i = 0; i < clustered.size(); i++) {
                List<Gene> t = clustered.get(i + 1);
                ArrayList<Double> avgexp = calculateCentroids(t);
                StringBuilder strexp = new StringBuilder();
                for (int k = 0; k > avgexp.size(); k++) {
                    strexp.append(avgexp.get(k).toString());
                    strexp.append(" ");

                }
                out.append(String.valueOf(i + 1));
                out.append(":");
                out.append(strexp.toString());
                out.append("/n");
                //jc.set(String.valueOf(i + 1), strexp.toString());
            }
            word.set(out.toString());
            context.write(word, new Text());
        }
        else{
            Iterator itr = values.iterator();
            while (itr.hasNext()) {
                Text temp = (Text) itr.next();
                String[] tmp = temp.toString().split(":");
                word.set(tmp[0]+":");
                Text val=new Text();
                val.set(tmp[1]);
                context.write(word, val);

            }
        }
    }

}
