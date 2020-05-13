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

import java.util.List;
import org.opendata.core.graph.ConnectedComponentGenerator;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.column.ExpandedColumn;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainComponentGenerator implements EdgeConsumer {
    
    private final ExpandedColumn _column;
    private final ConnectedComponentGenerator _compGen;
    private final int[] _nodeSizes;
    private final UniqueDomainSet _resultSet;

    public DomainComponentGenerator(
            ExpandedColumn column,
            ConnectedComponentGenerator compGen,
            UniqueDomainSet resultSet,
            int[] nodeSizes
    ) {
        _column = column;
        _compGen = compGen;
        _resultSet = resultSet;
        _nodeSizes = nodeSizes;
     }

    @Override
    public void close() {

        new MemUsagePrinter().print();
        
        for (IdentifiableIDSet comp : _compGen.getComponents()) {
            if (_column.originalNodes().overlaps(comp)) {
                boolean isDomain = true;
                if (comp.length() == 1) {
                    isDomain = (_nodeSizes[comp.first()] > 1);
                }
                if (isDomain) {
                    _resultSet.put(_column.id(), comp);
                }
            }
        }
    }

    @Override
    public void consume(int nodeId, List<Integer> edges) {

        _compGen.add(nodeId, edges);
    }
}
