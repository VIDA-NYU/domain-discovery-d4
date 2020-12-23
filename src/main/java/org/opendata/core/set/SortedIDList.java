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

import java.util.ArrayList;
import java.util.List;

/**
 * Sorted list of unique identifier. This class is a simple wrapper around a
 * sorted array of integers.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class SortedIDList implements Iterable<Integer> {

    /**
     * Get the array element at the given index position.
     * 
     * @param index
     * @return 
     */
    public abstract Integer get(int index);
    
    /**
     * Compute the intersection between tow sorted arrays. Returns a new sorted
     * array with the elements that occur in both arrays.
     * 
     * @param obj
     * @return 
     */
    public SortedIDList intersect(SortedIDList obj) {
        
        final int len1 = this.length();
        final int len2 = obj.length();
        
        int idx1 = 0;
        int idx2 = 0;

        List<Integer> overlap = new ArrayList<>();
        
        while ((idx1 < len1) && (idx2 < len2)) {
            int el1 = this.get(idx1);
            int comp = Integer.compare(el1, obj.get(idx2));
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                overlap.add(el1);
                idx1++;
                idx2++;
            }
        }
        
        return new SortedIDArray(overlap);
    }
    
    /**
     * Check if the length of the list is zero.
     *
     * @return 
     */
    public boolean isEmpty() {
        
        return this.length() == 0;
    }
    
    /**
     * Get the number of elements in this array.
     * 
     * @return 
     */
    public abstract int length();
    
    /**
     * Compute the overlap between two sorted array.
     * 
     * @param obj
     * @return 
     */
    public int overlap(SortedIDList obj) {
        
        final int len1 = this.length();
        final int len2 = obj.length();
        
        int idx1 = 0;
        int idx2 = 0;
        int overlap = 0;
        
        while ((idx1 < len1) && (idx2 < len2)) {
            int comp = Integer.compare(this.get(idx1), this.get(idx2));
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                overlap++;
                idx1++;
                idx2++;
            }
        }
        return overlap;
    }
}
