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

import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.signature.RobustSignature;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;

/**
 * Generator for local domains in an expanded column. Domains are generated as
 * connected components in an undirected graph where edges are defined by
 * reference in robust signatures.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class UndirectedDomainGenerator extends UndirectedConnectedComponents implements RobustSignatureConsumer {
    
    private final ExpandedColumn _column;
    private boolean _isDone = false;
    private final Integer[] _nodeSizes;
    private final UniqueDomainSet _resultSet;

    public UndirectedDomainGenerator(
            ExpandedColumn column,
            UniqueDomainSet resultSet,
            Integer[] nodeSizes
    ) {
        super(column.nodes());

        _column = column;
        _resultSet = resultSet;
        _nodeSizes = nodeSizes;
     }

    @Override
    public void close() {

        for (IdentifiableIDSet comp : this.getComponents()) {
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
    public void consume(RobustSignature sig) {

        if (_isDone) {
            return;
        }
        
        final int sigId = sig.id();
        
        if (_column.contains(sigId)) {
            for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                for (int nodeId : sig.get(iBlock)) {
                    if (_column.contains(nodeId)) {
                        this.edge(sigId, nodeId);
                    }
                }
            }
            if (this.isComplete()) {
                _isDone = true;
            }
        }
    }
    
    @Override
    public void open() {

    }
}
