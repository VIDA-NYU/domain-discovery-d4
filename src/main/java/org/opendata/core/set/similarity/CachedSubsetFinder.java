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
package org.opendata.core.set.similarity;

import java.util.HashMap;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Cached subset finder to avoid generating the same subset twice.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CachedSubsetFinder extends SubsetFinder {
    
    private final HashMap<Integer, IdentifiableObjectSet<IdentifiableIDSet>> _cache;
    private final IdentifiableObjectSet<IdentifiableIDSet> _elements;
    
    
    public CachedSubsetFinder(IdentifiableObjectSet<IdentifiableIDSet> elements) {
        
        super(elements);
        
        _elements = elements;
        _cache = new HashMap<>();
    }

    @Override
    public synchronized IdentifiableObjectSet<IdentifiableIDSet> getSubsetsFor(int id) {
        
        if (_cache.containsKey(id)) {
            return _cache.get(id);
        } else {
            IdentifiableObjectSet<IdentifiableIDSet> result;
            result = this.getSubsetsFor(_elements.get(id));
            _cache.put(id, result);
            return result;
        }
    }
}
