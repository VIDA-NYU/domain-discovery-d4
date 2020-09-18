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
package org.opendata.core.set;

import java.io.File;
import org.opendata.core.io.EntityBuffer;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.object.Entity;
import org.opendata.core.object.filter.ObjectFilter;
import org.opendata.db.eq.EQIndex;

/**
 * Set of identifiable entities.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EntitySet extends HashObjectSet<Entity> {
   
    public EntitySet() {
        
    }

    public EntitySet(File file) throws java.io.IOException {
	
        new EntitySetReader(file).read(new EntityBuffer(this));
    }
    
    public EntitySet(File file, ObjectFilter<Integer> filter) throws java.io.IOException {
	
        new EntitySetReader(file).read(filter, new EntityBuffer(this));
    }
    
    public EntitySet(File file, EQIndex eqIndex, IDSet nodes) throws java.io.IOException {
        
        HashIDSet filter = new HashIDSet();
        for (int nodeId : nodes) {
            filter.add(eqIndex.get(nodeId).terms());
        }
        new EntitySetReader(file).read(filter, new EntityBuffer(this));
    }
}
