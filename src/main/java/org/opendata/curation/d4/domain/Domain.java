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
package org.opendata.curation.d4.domain;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableIDSetWrapper;

/**
 * A domain is an identifiable object that has a list of column identifier and
 * a list of node identifier associated with it. The list of column identifier
 * are the columns in which the domain occurs. The list of nodes identify the
 * members of the domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Domain extends IdentifiableIDSetWrapper implements IdentifiableIDSet {
    
    private final IDSet _columns;
    private int[] _nodes;
    
    public Domain(int id, IDSet nodes, IDSet columns) {
        
        super(id, nodes);
        
        _columns = columns;
        
        _nodes = null;
    } 
    
    public IDSet columns() {
        
        return _columns;
    }
    
    public int[] nodes() {
    
        if (_nodes == null) {
            _nodes = this.toArray();
        }
        return _nodes;
    }
    
    public int termCount(int[] nodeSizes) {
        
        int count = 0;
        for (int nodeId : this) {
            count += nodeSizes[nodeId];
        }
        return count;
    }
}
