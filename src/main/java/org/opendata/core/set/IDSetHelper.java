/*
 * This file is part of the Data-Driven Domain Discovery Tool (D4).
 * 
 * Copyright (c) 2018-2020 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opendata.core.set;

/**
 * Collection of helper methods for ID sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IDSetHelper {
    
    public static boolean overlaps(ImmutableIDSet set1, ImmutableIDSet set2, int threshold) {

        int overlap = 0;

        int index1 = 0;
        int index2 = 0;

	final int len1 = set1.length();
	final int len2 = set2.length();
	
        while ((index1 < len1) && (index2 < len2)) {
            int comp = Integer.compare(set1.get(index1), set2.get(index2));
            if (comp < 0) {
                index1++;
		if ((overlap + (len1 - index1)) < threshold) {
		    return false;
		}
            } else if (comp > 0) {
                index2++;
		if ((overlap + (len2 - index2)) < threshold) {
		    return false;
		}
            } else {
                overlap++;
                if (overlap >= threshold) {
                    return true;
                }
                index1++;
                index2++;
            }
        }

        return false;
    }

    public static <T extends IDSet> IDSet union(Iterable<T> sets) {
        
        HashIDSet terms = new HashIDSet();
        
        for (T elements : sets) {
            terms.add(elements);
        }
        
        return terms;
    }
}
