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
package org.opendata.core.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public final class ArrayHelper {
    
    public static int[] arrayFromString(String value) {
        
        String[] tokens = value.split(",");
        int[] columns = new int[tokens.length];
        for (int iToken = 0; iToken < tokens.length; iToken++) {
            columns[iToken] = Integer.parseInt(tokens[iToken]);
        }
        return columns;
    }

    /**
     * Returns true if the first list contains the second list.
     * 
     * @param list1
     * @param list2
     * @return 
     */
    public static boolean contains(int[] list1, int[] list2) {
        
        if (list2.length > list1.length) {
            return false;
        }
        
        int idx1 = 0;
        int idx2 = 0;
        while ((idx1 < list1.length) && (idx2 < list2.length)) {
            int comp = Integer.compare(list1[idx1], list2[idx2]);
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                return false;
            } else {
                idx1++;
                idx2++;
            }
        }
        return (idx2 == list2.length);
    }

    /**
     * Returns true if the first list contains the given value.
     * 
     * @param list
     * @param value
     * @return 
     */
    public static boolean contains(int[] list, int value) {
        
        int left = 0;
        int right = list.length - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (list[mid] == value) {
                return true;
            }
            if (list[mid] < value) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return false;
    }

    public static boolean doOverlap(int[] list1, int[] list2) {
        
        final int len1 = list1.length;
        final int len2 = list2.length;

        int idx1 = 0;
        int idx2 = 0;
        while ((idx1 < len1) && (idx2 < len2)) {
            int comp = Integer.compare(list1[idx1], list2[idx2]);
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                return true;
            }
        }
        return false;
    }

    public static int overlap(int[] list1, int[] list2, Integer[] size) {
        
        final int len1 = list1.length;
        final int len2 = list2.length;
        
        int idx1 = 0;
        int idx2 = 0;
        int overlap = 0;
        
        while ((idx1 < len1) && (idx2 < len2)) {
            int comp = Integer.compare(list1[idx1], list2[idx2]);
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                overlap += size[list1[idx1]];
                idx1++;
                idx2++;
            }
        }
        return overlap;
    }

    public static int overlap(int[] list1, int[] list2, int minOverlap) {
        
        if ((list1.length < minOverlap) || (list1.length < minOverlap)) {
            return -1;
        }
        
        int overlap = 0;
        int idx1 = 0;
        int idx2 = 0;
        while ((idx1 < list1.length) && (idx2 < list2.length)) {
            int comp = Integer.compare(list1[idx1], list2[idx2]);
            if (comp < 0) {
            idx1++;
            } else if (comp > 0) {
            idx2++;
            } else {
            idx1++;
            idx2++;
            overlap++;
            }
                if ((overlap + Math.min(list1.length - idx1, list2.length - idx2)) < minOverlap) {
                    return -1;
                }
        }
        return overlap;
    }
    
    public static BigDecimal[] parseBigDecimalArray(String values) {

	String[] tokens = values.split(",");
	BigDecimal[] result = new BigDecimal[tokens.length];
	for (int iValue = 0; iValue < tokens.length; iValue++) {
	    result[iValue] = new BigDecimal(tokens[iValue]);
	}
	return result;
    }
    
    public static int[] parseIntArray(String values) {

	String[] tokens = values.split(",");
	int[] result = new int[tokens.length];
	for (int iValue = 0; iValue < tokens.length; iValue++) {
	    result[iValue] = Integer.parseInt(tokens[iValue]);
	}
	return result;
    }
    
    public static int[] toArray(List<Integer> values) {
        
        int[] result = new int[values.size()];
        for (int iVal = 0; iVal < values.size(); iVal++) {
            result[iVal] = values.get(iVal);
        }
        return result;
    }
}
