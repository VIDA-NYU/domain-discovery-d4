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
package org.opendata.core.value;

/**
 * Simple counter of value occurrences. Contains the text representation of the
 * value and a counter that can be incremented.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface ValueCounter extends Comparable<ValueCounter> {
    
    /**
     * Current value count.
     * 
     * @return 
     */
    public int getCount();

    /**
     * Text representation of the value.
     * 
     * @return 
     */
    public String getText();
    
    /**
     * Increment the value count by 1.
     * 
     * @return	Count after increment
     */
    public int incCount();
    
    /**
     * Increment value counter by given value.
     * 
     * @param increment	Value to increment counter with.
     * @return	Count after increment
     */
    public int incCount(int increment);
    
    public boolean isEmpty();
}
