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
 * Implements the basic methods of object sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public abstract class ObjectSetImpl <T> implements ObjectSet<T> {
    
    @Override
    public boolean contains(ObjectSet<T> values) {

        if (this.length() >= values.length()) {
            for (T value : values) {
                if (!this.contains(value)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    @Override
    public boolean isTrueSubsetOf(ObjectSet<T> list) {
	
	if (this.length() < list.length()) {
	    for (T element : this) {
		if (!list.contains(element)) {
		    return false;
		}
	    }
            return true;
	}
        return false;
    }

    @Override
    public int overlap(ObjectSet<T> list) {

	ObjectSet<T> innerList;
	ObjectSet<T> outerList;
	
        if (this.length() > list.length()) {
	    outerList = list;
	    innerList = this;
	} else {
	    outerList = this;
	    innerList = list;
	}
	
	int count = 0;
	
        for (T element : outerList) {
            if (innerList.contains(element)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean overlaps(ObjectSet<T> list) {

	ObjectSet<T> innerList;
	ObjectSet<T> outerList;
	
        if (this.length() > list.length()) {
	    outerList = list;
	    innerList = this;
	} else {
	    outerList = this;
	    innerList = list;
	}
	
        for (T element : outerList) {
            if (innerList.contains(element)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean sameSetAs(ObjectSet<T> list) {

	if (this.length() == list.length()) {
	    for (T element : this) {
		if (!list.contains(element)) {
		    return false;
		}
	    }
	    return true;
	}
	return false;
    }
}
