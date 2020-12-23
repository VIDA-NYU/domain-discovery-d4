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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.set.SortedIDSet;
import org.opendata.core.util.IdentifiableCount;

/**
 * COllection of helper methods for database column and column files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public final class ColumnHelper {
    
    /**
     * Static method to convert a serialization of columnId:frequency pairs
     * to a list of identifiable integer. The elements in the list are not
     * necessarily sorted. This method therefore returns a list instead of
     *  a sorted ID set,
     * 
     * This is the reverse operation from toStringArray.
     * 
     * @param text
     * @return 
     */
    public static List<IdentifiableInteger> fromArbitraryArrayString(String text) {
        
        List<IdentifiableInteger> elements = new ArrayList<>();
        for (String pair : text.split(",")) {
            int pos = pair.indexOf(":");
            int id = Integer.parseInt(pair.substring(0, pos));
            int val = Integer.parseInt(pair.substring(pos + 1));
            elements.add(new IdentifiableCount(id, val));
        }
        return elements;
    }

    /**
     * Static method to convert a serialization of columnId:frequency pairs
     * to a sorted ID set if identifiable integer.
     * 
     * This is the reverse operation from toStringArray.
     * 
     * @param text
     * @return 
     */
    public static SortedIDSet<IdentifiableInteger> fromSortedArrayString(String text) {
        
        String[] tokens = text.split(",");
        IdentifiableInteger[] elements = new IdentifiableInteger[tokens.length];
        for (int iToken = 0; iToken < tokens.length; iToken++) {
            String pair = tokens[iToken];
            int pos = pair.indexOf(":");
            int id = Integer.parseInt(pair.substring(0, pos));
            int val = Integer.parseInt(pair.substring(pos + 1));
            elements[iToken] = new IdentifiableCount(id, val);
        }
        return new SortedIDSet<>(elements);
    }

    /**
     * Get the column identifier from the column name. It is expected that the
     * column identifier is a number at the start of the file name.
     * 
     * @param file
     * @return 
     */
    public static int getColumnId(File file) {
        
        String[] tokens = file.getName().split("\\.");
        try {
            return Integer.parseInt(tokens[0]);
        } catch (java.lang.NumberFormatException ex) {
        }
        return Integer.parseInt(tokens[2]);
    }
    
    /**
     * Convert a list of column counts into a string representation.
     * 
     * @param <T>
     * @param elements
     * @return 
     */
    public static <T extends IdentifiableInteger> String toArrayString(Iterable<T> elements) {
        
	StringBuilder buf = new StringBuilder();
	
        Iterator<T> iterator = elements.iterator();
	while (iterator.hasNext()) {
            IdentifiableInteger el = iterator.next();
            if (buf.length() > 0) {
                buf.append(",");
            }
	    buf.append(el.id()).append(":").append(el.value());
	}
	
	return buf.toString();
    }
}
