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
package org.opendata.db.eq;

import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.object.IdentifiableObject;

/**
 * Each equivalence class is a set of terms that always occur together in the
 * same set of columns. Equivalence classes are the basis for all steps in the
 * domain discovery process. This interface defines methods that provide
 * access to information about equivalence classes that is required by the
 * different steps in the discovery algorithm.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface EQ extends IdentifiableObject {
   
    /**
     * Get a sorted array of identifier for columns that contain the equivalence
     * class.
     * 
     * @return 
     */
    public Integer[] columns();
    
    /**
     * Get the number of columns that contain this equivalence class.
     * 
     * @return 
     */
    public int columnCount();

    /**
     * Get sorted list of columns together with the frequency of the equivalence
     * class in that column.
     * 
     * @return 
     */
    public IdentifiableInteger[] columnFrequencies();
    
    /**
     * Get a sorted array of identifier for terms that the equivalence class
     * contains.
     * 
     * @return 
     */
    public Integer[] terms();
   
    /**
     * Get number of terms in the equivalence class.
     * 
     * @return 
     */
    public int termCount();
}
