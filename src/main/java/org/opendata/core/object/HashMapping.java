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
package org.opendata.core.object;

import java.util.HashMap;

/**
 * Implements the identifier mapping using a hash map. Expects a map where the
 * keys are the source set and the values represent the target set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class HashMapping implements IdentifierMapping {

    private final HashMap<Integer, Integer> _mapping;
    
    public HashMapping(HashMap<Integer, Integer> mapping) {
        
        _mapping = mapping;
    }

    @Override
    public int getIdentifier(int objId) {

        for (int key : _mapping.keySet()) {
            if (_mapping.get(key) == objId) {
                return key;
            }
        }
        return -1;
    }
    
    @Override
    public int getValue(int id) {

        return _mapping.get(id);
    }
}
