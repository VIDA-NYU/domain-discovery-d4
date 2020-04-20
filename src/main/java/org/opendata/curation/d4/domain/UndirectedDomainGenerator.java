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
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.graph.components.UndirectedConnectedComponents;
import org.opendata.core.set.IdentifiableIDSet;

/**
 * Generator for local domains in an expanded column. Domains are generated as
 * connected components in an undirected graph where edges are defined by
 * reference in robust signatures.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class UndirectedDomainGenerator extends UndirectedConnectedComponents implements SignatureBlocksConsumer {
    
    private final ExpandedColumn _column;
    private final int[] _nodeSizes;
    private final UniqueDomainSet _resultSet;

    public UndirectedDomainGenerator(
            ExpandedColumn column,
            UniqueDomainSet resultSet,
            int[] nodeSizes
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
    public void consume(SignatureBlocks sig) {

        final int sigId = sig.id();
        
        if (_column.contains(sigId)) {
            for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                for (int nodeId : sig.get(iBlock)) {
                    if (this.contains(nodeId)) {
                        this.edge(sigId, nodeId);
                    }
                }
            }
        }
    }

    @Override
    public void open() {

    }
}
