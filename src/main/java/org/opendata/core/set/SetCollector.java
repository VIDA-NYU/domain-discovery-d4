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

import org.opendata.core.object.IdentifiableObject;

/**
 * Collector for identifier sets that are associated with an identifiable
 * object.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SetCollector extends HashObjectSet<MutableIdentifiableIDSet> {
   
    public void add(IdentifiableObject obj, int id) {
        
        if (this.contains(obj.id())) {
            this.get(obj.id()).add(id);
        } else {
            this.add(new MutableIdentifiableIDSet(obj.id(), id));
        }
    }
    
    public void add (IdentifiableObject obj, IDSet ids) {
        
        if (this.contains(obj.id())) {
            this.get(obj.id()).add(ids);
        } else {
            this.add(new MutableIdentifiableIDSet(obj.id(), ids));
        }
    }
}
