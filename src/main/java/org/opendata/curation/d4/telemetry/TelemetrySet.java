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
package org.opendata.curation.d4.telemetry;

import java.util.HashMap;

/**
 * Maintain a set of runtime (in milliseconds) for different D4 components.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TelemetrySet implements TelemetryCollector {

    private final HashMap<String, Long> _values = new HashMap<>();
    
    @Override
    public void add(String key, long execTime) {

        if (_values.containsKey(key)) {
            _values.put(key, this.get(key) + execTime);
        } else {
            _values.put(key, execTime);
        }
        System.out.println("D4 (TELEMETRY) - " + key + ": " + execTime);
    }
    
    public long get(String key) {
        
        return _values.get(key);
    }
}
