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
import org.opendata.core.object.IdentifiableObject;

/**
 * Sorted array of identifier.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class SortedIDSet <T extends IdentifiableObject> implements Iterable<T> {
   
    private final T[] _elements;

    public SortedIDSet(T[] elements) {

        for (int i = 1; i < elements.length; i++) {
            if (elements[i - 1].id() >= elements[i].id()) {
                throw new IllegalArgumentException(
                        String.format(
                                "[%d:%d] = %d:%d",
                                (i - 1),
                                i,
                                elements[i - 1].id(),
                                elements[i].id()
                        )
                );
            }
        }

        _elements = elements;        
    }
    
    public T get(int index) {
        
        return _elements[index];
    }
    
    @Override
    public Iterator<T> iterator() {

        return Arrays.asList(_elements).iterator();
    }
    
    public String key() {

        if (_elements.length == 0) {
            return "";
        }
        
        StringBuilder buf = new StringBuilder(Integer.toString(_elements[0].id()));
        for (int iColumn = 1; iColumn < _elements.length; iColumn++) {
            buf.append(",").append(_elements[iColumn].id());
        }
        return buf.toString();
    }
    
    public int length() {
        
        return _elements.length;
    }
}
