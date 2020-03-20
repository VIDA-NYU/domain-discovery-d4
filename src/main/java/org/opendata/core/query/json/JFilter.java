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

import com.google.gson.JsonObject;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JFilter {
    
    private final String _condition;
    private final boolean _ignoreCase;
    private final JQuery _query;
    
    public JFilter(JQuery query, String condition, boolean ignoreCase) {
        
        _query = query;
        _condition = condition;
        _ignoreCase = ignoreCase;
    }
    
    public JFilter(JQuery query, String condition) {
        
        this(query, condition, false);
    }

    public JFilter(String query, String condition, boolean ignoreCase) {
        
        this(new JQuery(query), condition, ignoreCase);
    }
    
    public JFilter(String query, String condition) {
        
        this(new JQuery(query), condition, false);
    }
    
    public boolean eval(JsonObject doc) {
        
        String val = _query.eval(doc).getAsString();
        if (val != null) {
            if (_ignoreCase) {
                return val.equalsIgnoreCase(_condition);
            } else {
                return val.equals(_condition);
            }
        } else {
            return (_condition == null);
        }
    }
}
