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

import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Implement a linked list of ordered elements.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class OrderedLinkedList<T extends Comparable<T>> extends LinkedList<T> {
    
    /**
     * Sorted insert. Inserts the given element at the position defined by the
     * order of the existing elements.
     * 
     * @param element 
     */
    public void insert(T element) {      
        
        ListIterator<T> itr = listIterator();
        while(itr.hasNext()) {
            T elementInList = itr.next();
            if (elementInList.compareTo(element) >= 0) {
                itr.previous();
                itr.add(element);
                return;
            }
        }
        itr.add(element);
    }    
}
