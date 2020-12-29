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
package org.opendata.db;

import com.google.gson.JsonObject;
import org.opendata.core.object.IdentifiableObject;

/**
 * Interface for collections of terms from an equivalence class. The collection
 * may represent the full equivalence class or just a sample of the total terms.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface EQTerms extends IdentifiableObject, Iterable<String> {
    
    /**
     * Add a term to the set.
     * 
     * @param term 
     */
    public void add(String term);
    
    /**
     * Get the term in this set at the given index position.
     * 
     * @param index
     * @return 
     */
    public String get(int index);
    
    /**
     * Number of therms in this set.
     * 
     * @return 
     */
    public int size();
    
    /**
     * Total number of terms in the equivalence class.
     * 
     * @return 
     */
    public int termCount();
    
    /**
     * Get Json serialization.
     * 
     * @return 
     */
    public JsonObject toJsonObject();
}
