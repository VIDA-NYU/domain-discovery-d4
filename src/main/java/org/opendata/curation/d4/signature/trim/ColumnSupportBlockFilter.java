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
package org.opendata.curation.d4.signature.trim;

import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.constraint.ZeroThreshold;
import org.opendata.core.object.filter.AnyObjectFilter;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.signature.SignatureBlocksImpl;
import org.opendata.db.eq.EQIndex;

/**
 * Filter signature blocks based on column support. Includes only those blocks
 * the contain nodes that all occur together in at least one column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnSupportBlockFilter extends SignatureTrimmer {

    private final EQIndex _eqIndex;
    private final int _minStart;
    
    public ColumnSupportBlockFilter(
            EQIndex eqIndex,
            int minStart,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        super(new AnyObjectFilter(), nonEmptyConstraint, consumer);
        
        _eqIndex = eqIndex;
        _minStart = minStart;
    }

    public ColumnSupportBlockFilter(
            EQIndex eqIndex,
            int minStart,
            SignatureBlocksConsumer consumer
    ) {
        
        this(eqIndex, minStart, new ZeroThreshold(), consumer);
    }

    public ColumnSupportBlockFilter(
            EQIndex eqIndex,
            SignatureBlocksConsumer consumer
    ) {
        
        this(eqIndex, 0, new ZeroThreshold(), consumer);
    }

    @Override
    public void trim(SignatureBlocks sig, SignatureBlocksConsumer consumer) {

        IDSet nodeColumns = _eqIndex.get(sig.id()).columns();
        
        int lastIndex = 0;
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            IDSet columns = nodeColumns;
            for (int nodeId : sig.get(iBlock)) {
                columns = columns.intersect(_eqIndex.get(nodeId).columns());
                if (columns.isEmpty()) {
                    break;
                }
            }
            if (columns.isEmpty()) {
                break;
            }
            lastIndex++;
        }
        int sigSize = Math.max(_minStart, lastIndex);
        int[][] blocks = new int[sigSize][];
        for (int iBlock = 0; iBlock < blocks.length; iBlock++) {
            blocks[iBlock] = sig.get(iBlock);
        }
        consumer.consume(new SignatureBlocksImpl(sig.id(), sig.maxSim(), blocks));
    }
}
