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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Sorted list of unique identifier. This class is a simple wrapper around a
 * sorted array of integers.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SortedIDArray extends SortedIDList{
   
    private final Integer[] _elements;

    /**
     * Initialize the sorted array. Raises an exception if the given array
     * elements are not distinct and sorted in ascending order.
     * 
     * @param elements 
     */
    public SortedIDArray(Integer[] elements) {
        
        for (int i = 1; i < elements.length; i++) {
            if (elements[i - 1] >= elements[i]) {
                throw new IllegalArgumentException(
                        String.format(
                                "[%d:%d] = %d:%d",
                                (i - 1),
                                i,
                                elements[i - 1],
                                elements[i]
                        )
                );
            }
        }

        _elements = elements;
    }

    public SortedIDArray(List<Integer> elements) {
    
        _elements = new Integer[elements.size()];
        
        if (!elements.isEmpty()) {
            _elements[0] = elements.get(0);
            for (int i = 1; i < elements.size(); i++) {
                _elements[i] = elements.get(i);
                if (_elements[i - 1] >= _elements[i]) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "[%d:%d] = %d:%d",
                                    (i - 1),
                                    i,
                                    _elements[i - 1],
                                    _elements[i]
                            )
                    );
                }
            }
        }
    }
    
    @Override
    public Integer get(int index) {

        return _elements[index];
    }

    @Override
    public Iterator<Integer> iterator() {

        return Arrays.asList(_elements).iterator();
    }
    
    public int length() {
        
        return _elements.length;
    }
}
