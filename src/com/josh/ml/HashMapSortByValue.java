
package com.josh.ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HashMapSortByValue {

	public static void main(String[] args){
		HashMap<String, Double> hm = new HashMap<String, Double>();
		hm.put("A", 3.0);
		hm.put("B", 1.0);
		hm.put("C", 2.0);
		
		List<Entry<String, Double>> entries =  new ArrayList<Map.Entry<String, Double>>(hm.entrySet());
		
		Collections.sort(entries, new Comparator<Map.Entry<String, Double>>(){
            public int compare(Map.Entry<String, Double> entry1, Map.Entry<String, Double> entry2){
                return (entry1.getValue().compareTo(entry2.getValue()));
            }
        });

		for (Map.Entry<String, Double> entry:entries) {
			System.out.printf("%s, %f\n", entry.getKey(), entry.getValue());
        }

	}

}
