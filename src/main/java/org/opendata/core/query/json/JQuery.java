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
import com.google.gson.JsonObject;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JQuery {
    
    private final JPath _path;
    
    public JQuery(JPath path) {
        
        _path = path;
    }
    
    public JQuery(String path) {
        
        this(new JPath(path));
    }
    
    public JsonElement eval(JsonObject doc) {
        
        JsonObject obj = doc;
        for (int iComp = 0; iComp < _path.size(); iComp++) {
            String key = _path.get(iComp);
            if (obj.has(key)) {
                if (iComp == (_path.size() - 1)) {
                    return obj.get(key);
                } else if (obj.get(key).isJsonObject()) {
                    obj = obj.get(key).getAsJsonObject();
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
        return null;
    }
}
