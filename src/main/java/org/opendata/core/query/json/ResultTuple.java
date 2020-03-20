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
package org.opendata.core.query.json;

import com.google.gson.JsonElement;
import java.util.HashMap;
import org.opendata.core.util.StringHelper;

/**
 * Tuple in the result set of a Json query. Each tuple is a list of values that
 * can either be accessed by their index position of the column key.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ResultTuple {
    
    private final HashMap<String, Integer> _columns;
    private final JsonElement[] _values;
    
    public ResultTuple(JsonElement[] values, HashMap<String, Integer> columns) {
        
        _values = values;
        _columns = columns;
    }
    
    /**
     * Access result cell value by index.
     * 
     * @param index
     * @return 
     */
    public JsonElement get(int index) {
        
        return _values[index];
    }
    
    /**
     * Access result cell by column name.
     * 
     * @param column
     * @return 
     */
    public JsonElement get(String column) {
        
        return _values[_columns.get(column)];
    }
    
    /**
     * Get string representation for value at the given index position.
     * 
     * @param index
     * @return 
     */
    public String getAsString(int index) {
        
        JsonElement el = _values[index];
        if (el == null) {
            return "";
        } else if (el.isJsonPrimitive()) {
            return el.getAsString();
        } else {
            return el.toString();
        }
    }
    
    /**
     * Get string representation for value of the given column.
     * 
     * @param column
     * @return 
     */
    public String getAsString(String column) {
        
        return this.getAsString(_columns.get(column));
    }
    
    /**
     * Join all tuple values into a single string with the given delimiter.
     * 
     * @param delimiter
     * @return 
     */
    public String join(String delimiter) {
       
        String[] values = new String[_values.length];
        for (int iColumn = 0; iColumn < _values.length; iColumn++) {
            values[iColumn] = this.getAsString(iColumn);
        }
        return StringHelper.joinStrings(values, delimiter);
    }
    
    public int size() {
        
        return _values.length;
    }
}
