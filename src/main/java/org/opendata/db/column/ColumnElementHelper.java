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
package org.opendata.db.column;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.opendata.core.util.count.IdentifiableCount;
import org.opendata.core.set.ImmutableObjectSet;

/**
 * Helper methods for column elements.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public final class ColumnElementHelper {
    
    /**
     * Static method to convert a list of columnId:frequency pairs into an
     * immutable identifiable count set.
     * 
     * This is the reverse operation from toStringArray.
     * 
     * @param value
     * @return 
     */
    public static ImmutableObjectSet<IdentifiableCount> fromStringArray(String value) {
        
        String[] pairs = value.split(",");
        
        IdentifiableCount[] result = new IdentifiableCount[pairs.length];
        for (int iPair = 0; iPair < pairs.length; iPair++) {
            result[iPair] = new IdentifiableCount(pairs[iPair]);
        }
        return new ImmutableObjectSet<>(result);
    }
    
    /**
     * Convert a list of column counts into a string representation.
     * 
     * @param elements
     * @return 
     */
    public static String toArrayString(List<IdentifiableCount> elements) {
        
	StringBuilder buf = new StringBuilder();
	
	if (elements.size() > 0) {
            Collections.sort(elements, new Comparator<IdentifiableCount>() {
                @Override
                public int compare(IdentifiableCount o1, IdentifiableCount o2) {
                    return Integer.compare(o1.id(), o2.id());
                }
            });
	    buf.append(elements.get(0).toPairString());
	    for (int iElement = 1; iElement < elements.size(); iElement++) {
		buf.append(",").append(elements.get(iElement).toPairString());
	    }
	}
	
	return buf.toString();
    }
}
