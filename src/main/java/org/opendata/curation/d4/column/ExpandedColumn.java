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
package org.opendata.curation.d4.column;

import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.IDSet;

/**
 * Expanded column maintains a list of nodes that were in the original column
 * together with list of nodes in the expansion.
 * 
 * The expansion set can be empty.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class ExpandedColumn extends IdentifiableObjectImpl {
    
    private final IDSet _expansion;
    private final IDSet _nodes;

    public ExpandedColumn(int id, IDSet nodes, IDSet expansion) {
        
        super(id);
        
        _nodes = nodes;
        _expansion = expansion;
    }

    public abstract boolean contains(int id);
    public abstract ExpandedColumn expand(IDSet nodes);

    public IDSet expandedNodes() {

        return _expansion;
    }

    public int expansionSize() {

        return _expansion.length();
    }

    public boolean isColumnNode(int id) {

        return _nodes.contains(id);
    }

    public IDSet nodes() {
        
        return _nodes.union(_expansion);
    }
    
    public IDSet originalNodes() {

        return _nodes;
    }
}
