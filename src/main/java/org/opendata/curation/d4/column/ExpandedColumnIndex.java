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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableIDSetWrapper;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Create an unique index of expanded columns. Two expanded columns are
 * considered the same if their original node set is he same (since they would
 * have been expanded in the same way in this case). Maintains a list of
 * identifier for sets of columns that are the same.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnIndex implements ExpandedColumnConsumer {

    private HashMap<String, Integer> _columnIndex;
    private List<ExpandedColumn> _columnList = null;
    private HashMap<Integer, HashIDSet> _columnMapping;
    
    public ExpandedColumnIndex() {
        
    }
    
    public ExpandedColumnIndex(ExpandedColumn column) {
        
        this.open();
        this.consume(column);
        this.close();
    }
    
    @Override
    public final void close() {

    }

    public IDSet columns(int id) {
    
        if (_columnMapping.containsKey(id)) {
            return _columnMapping.get(id);
        } else {
            return new HashIDSet(id);
        }
    }
    
    public List<ExpandedColumn> columns() {
        
        return _columnList;
    }
    
    @Override
    public final void consume(ExpandedColumn column) {

        String key = column.originalNodes().toIntString();
        if (!_columnIndex.containsKey(key)) {
            _columnList.add(column);
            _columnIndex.put(key, column.id());
        } else {
            int columnId = _columnIndex.get(key);
            if (!_columnMapping.containsKey(columnId)) {
                HashIDSet columns = new HashIDSet(columnId);
                columns.add(column.id());
                _columnMapping.put(columnId, columns);
            } else {
                _columnMapping.get(columnId).add(column.id());
            }
        }
    }

    @Override
    public final void open() {

        _columnIndex = new HashMap<>();
        _columnList = new ArrayList<>();
        _columnMapping = new HashMap<>();
    }
    
    public IdentifiableObjectSet<IdentifiableIDSet> toColumns(boolean originalOnly) {
        
        HashObjectSet<IdentifiableIDSet> result = new HashObjectSet<>();
        
        for (ExpandedColumn column : _columnList) {
            IDSet nodes;
            if (originalOnly) {
                nodes = column.originalNodes();
            } else {
                nodes = column.nodes();
            }
            for (int columnId : this.columns(column.id())) {
                result.add(new IdentifiableIDSetWrapper(columnId, nodes));
            }
        }
        
        return result;
    }
}
