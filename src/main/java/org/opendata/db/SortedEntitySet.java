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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.opendata.core.object.Entity;
import org.opendata.core.sort.NamedObjectComparator;

/**
 * Sorted list of entities (e.g., database terms).
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SortedEntitySet implements Iterable<Entity> {
   
    private final List<Entity> _entities;
    
    public SortedEntitySet(HashMap<Integer, EQTerms> eqs) {
        
        _entities = new ArrayList<>();
        for (EQTerms eq : eqs.values()) {
            for (String term : eq) {
                _entities.add(new Entity(eq.id(), term));
            }
        }
        Collections.sort(_entities, new NamedObjectComparator());
    }

    @Override
    public Iterator<Entity> iterator() {
        
        return _entities.iterator();
    }
}
