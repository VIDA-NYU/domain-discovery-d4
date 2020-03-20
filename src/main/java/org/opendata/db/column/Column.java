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
package org.opendata.db.column;

import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.MutableIdentifiableIDSet;
import org.opendata.db.eq.EQ;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Column extends MutableIdentifiableIDSet {

    public Column(int id) {
    
        super(id);
    }

    public Column(int id, IDSet values) {
        
        super(id, values);
    }
    
    public <T extends EQ> int termCount(IdentifiableObjectSet<T> nodes) {
        
        int count = 0;
        for (int nodeId : this) {
            count += nodes.get(nodeId).termCount();
        }
        return count;
    }
    
    public <T extends EQ> IDSet terms(IdentifiableObjectSet<T> nodes) {
        
        HashIDSet terms = new HashIDSet();
        for (int nodeId : this) {
            terms.add(nodes.get(nodeId).terms());
        }
        return terms;
    }
}