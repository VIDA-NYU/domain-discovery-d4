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

import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.db.column.Column;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MutableExpandedColumn extends ExpandedColumn {

    private final boolean[] _nodeIndex;
    private final int _offset;
    
    public MutableExpandedColumn(int id, IDSet nodes, IDSet expansion) {
        
        super(id, nodes, expansion);
        
        int minId = Integer.MAX_VALUE;
        int maxId = -1;
        
        IDSet allNodes =  nodes.union(expansion);
        for (int nodeId : allNodes) {
            if (nodeId < minId) {
                minId = nodeId;
            }
            if (nodeId > maxId) {
                maxId = nodeId;
            }
        }
        _offset = minId;
        _nodeIndex = new boolean[(maxId - minId) + 1];
        for (int nodeId : allNodes) {
            _nodeIndex[nodeId - _offset] = true;
        }
    }

    public MutableExpandedColumn(Column column) {
        
        this(column.id(), column, new HashIDSet());
    }
    
    @Override
    public boolean contains(int id) {

        int index = id - _offset;
        if ((index >= 0) && (index < _nodeIndex.length)) {
            return _nodeIndex[index];
        } else {
            return false;
        }
    }

    @Override
    public ExpandedColumn expand(IDSet nodes) {

        return new MutableExpandedColumn(
                this.id(),
                this.originalNodes(),
                this.expandedNodes().union(nodes)
        );
    }
}
