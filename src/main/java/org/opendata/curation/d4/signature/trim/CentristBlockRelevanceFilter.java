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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.constraint.ZeroThreshold;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.object.filter.AnyObjectFilter;
import org.opendata.core.prune.CandidateSetFinder;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.sort.DoubleValueDescSort;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Liberal signature blocks trimmer. The liberal trimmer prunes all
 * blocks starting from the block with the most elements. Only if the first
 * block is the largest block it will not be pruned.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CentristBlockRelevanceFilter extends SignatureTrimmer {

    private final CandidateSetFinder<IdentifiableDouble> _dropFinder;
    private final EQIndex _eqIndex;
    private final BlockScoreFunction _scoreFunc;
    
    public CentristBlockRelevanceFilter(
            EQIndex eqIndex,
            BlockScoreFunction scoreFunc,
            CandidateSetFinder<IdentifiableDouble> dropFinder,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        super(new AnyObjectFilter(), nonEmptyConstraint, consumer);
        
        _eqIndex = eqIndex;
        _scoreFunc = scoreFunc;
        _dropFinder = dropFinder;
    }
    
    public CentristBlockRelevanceFilter(
            EQIndex eqIndex,
            BlockScoreFunction scoreFunc,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        
        this(
                eqIndex,
                scoreFunc,
                new MaxDropFinder<>(
                    new GreaterThanConstraint(BigDecimal.ZERO),
                    false,
                    false
                ),
                nonEmptyConstraint,
                consumer
        );
    }

    public CentristBlockRelevanceFilter(
            EQIndex eqIndex,
            BlockScoreFunction scoreFunc,
            SignatureBlocksConsumer consumer
    ) {
        
        this(eqIndex, scoreFunc, new ZeroThreshold(), consumer);
    }

    public CentristBlockRelevanceFilter(
            EQIndex eqIndex,
            IdentifiableObjectSet<Column> columns,
            SignatureBlocksConsumer consumer
    ) {
        
        this(eqIndex, new PrecisionScore(eqIndex, columns), new ZeroThreshold(), consumer);
    }

    @Override
    public void trim(SignatureBlocks sig, SignatureBlocksConsumer consumer) {

        IDSet columns = _eqIndex.get(sig.id()).columns();
        
        List<IdentifiableDouble> elements = new ArrayList<>();
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            int[] block = sig.get(iBlock);
            Arrays.sort(block);
            BigDecimal score = _scoreFunc.maxScore(block, columns);
            elements.add(new IdentifiableDouble(iBlock, score));
        }
        Collections.sort(elements, new DoubleValueDescSort());
        int dropIndex = _dropFinder.getPruneIndex(elements);
        if (dropIndex > 0) {
            if (elements.get(0).value() > 0) {
                for (int i = 0; i < elements.size(); i++) {
                    IdentifiableDouble e = elements.get(i);
                }
                consumer.consume(new CentristSignature(sig, elements, dropIndex));
            }
        }
    }
}
