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
package org.opendata.db.tools;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Record of distinct and total values in a column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnStats {
   
    private int _distinctCount;
    private int _totalCount;
    
    public ColumnStats(int distinctCount, int totalCount) {
        
        _distinctCount = distinctCount;
        _totalCount = totalCount;
    }
    
    public ColumnStats() {
        
        this(0, 0);
    }
    
    public int distinctCount() {
        
        return _distinctCount;
    }
    
    public void inc(int count) {
        
        _distinctCount++;
        _totalCount += count;
    }
    
    public JsonObject toJson() {
        
        JsonObject doc = new JsonObject();
        doc.add("distinctCount", new JsonPrimitive(_distinctCount));
        doc.add("totalCount", new JsonPrimitive(_totalCount));
        return doc;
    }
    
    @Override
    public String toString() {
        
        return _distinctCount + "\t" + _totalCount;
    }
    
    public int totalCount() {
    
        return _totalCount;
    }
}
