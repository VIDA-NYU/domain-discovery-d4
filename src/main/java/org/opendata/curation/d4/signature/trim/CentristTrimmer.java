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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.constraint.ZeroThreshold;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.prune.CandidateSetFinder;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.IDSet;
import org.opendata.core.sort.DoubleValueDescSort;
import org.opendata.db.eq.EQHelper;

/**
 * Centrist signature blocks trimmer. The centrist trimmer uses a scoring
 * function to assign weights to each signature block based on overlap with
 * elements in the given column. It uses a drop finder to then decide which
 * blocks to keep or prune.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CentristTrimmer extends SignatureTrimmer {

    private final int[] _column;
    private final int _columnSize;
    private final CandidateSetFinder<IdentifiableDouble> _dropFinder;
    private final BlockScoreFunction _func;
    private final int[] _nodeSizes;
    
    public CentristTrimmer(
            IDSet column,
            int[] nodeSizes,
            BlockScoreFunction func,
            CandidateSetFinder<IdentifiableDouble> dropFinder,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        super(column, nonEmptyConstraint, consumer);
        
        _column = column.toArray();
        _nodeSizes = nodeSizes;
        _func = func;
        _dropFinder = dropFinder;
        
        _columnSize = EQHelper.setSize(_column, nodeSizes);
    }

    public CentristTrimmer(
            IDSet column,
            int[] nodeSizes,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
    
        this(
                column,
                nodeSizes,
                new PrecisionScore(),
                new MaxDropFinder<>(
                        new GreaterThanConstraint(BigDecimal.ZERO),
                        false,
                        false
                ),
                nonEmptyConstraint,
                consumer
        );
    }

    public CentristTrimmer(
            IDSet column,
            int[] nodeSizes,
            SignatureBlocksConsumer consumer
    ) {
    
        this(
                column,
                nodeSizes,
                new ZeroThreshold(),
                consumer
        );
    }

    @Override
    public void trim(SignatureBlocks sig, SignatureBlocksConsumer consumer) {

        List<IdentifiableDouble> elements = new ArrayList<>();
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            final int[] block = sig.get(iBlock);
            final int len1 = block.length;
            final int len2 = _column.length;
            int idx1 = 0;
            int idx2 = 0;
            int blSize = 0;
            int overlap = 0;
            while ((idx1 < len1) && (idx2 < len2)) {
                final int nodeId = block[idx1];
                int comp = Integer.compare(nodeId, _column[idx2]);
                if (comp < 0) {
                    blSize += _nodeSizes[nodeId];
                    idx1++;
                } else if (comp > 0) {
                    idx2++;
                } else {
                    int nodeSize = _nodeSizes[nodeId];
                    blSize += nodeSize;
                    overlap += nodeSize;
                    idx1++;
                    idx2++;
                }
            }
            if (overlap > 0) {
                while (idx1 < len1) {
                    blSize += _nodeSizes[block[idx1++]];
                }
                BigDecimal val = _func.relevance(_columnSize, blSize, overlap);
                elements.add(new IdentifiableDouble(iBlock, val.doubleValue()));
            } else {
                elements.add(new IdentifiableDouble(iBlock, 0.0));
            }
        }
        Collections.sort(elements, new DoubleValueDescSort<IdentifiableDouble>());
        int dropIndex = _dropFinder.getPruneIndex(elements);
        if (dropIndex > 0) {
            if (elements.get(0).value() > 0) {
                consumer.consume(new CentristSignature(sig, elements, dropIndex));
            }
        }
    }
}
