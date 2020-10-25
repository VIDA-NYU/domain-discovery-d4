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
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.sort.DoubleValueDescSort;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Centrist signature blocks trimmer. The centrist trimmer uses a scoring
 * function to assign weights to each signature block based on overlap with
 * elements in the given column. It uses a drop finder to then decide which
 * blocks to keep or prune.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CentristTrimmer extends SignatureTrimmer {

    private final int _columnId;
    private final CandidateSetFinder<IdentifiableDouble> _dropFinder;
    private final BlockScoreFunction _scoreFunc;

    public CentristTrimmer(
            IdentifiableIDSet column,
            BlockScoreFunction scoreFunc,
            CandidateSetFinder<IdentifiableDouble> dropFinder,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        super(column, nonEmptyConstraint, consumer);
        
        _columnId = column.id();
        _scoreFunc = scoreFunc;
        _dropFinder = dropFinder;
    }

    public CentristTrimmer(
            EQIndex eqIndex,
            IdentifiableObjectSet<Column> columns,
            IdentifiableIDSet column,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
    
        this(
                column,
                new PrecisionScore(eqIndex, columns),
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
            EQIndex eqIndex,
            IdentifiableObjectSet<Column> columns,
            IdentifiableIDSet column,
            SignatureBlocksConsumer consumer
    ) {
    
        this(
                eqIndex,
                columns,
                column,
                new ZeroThreshold(),
                consumer
        );
    }

    public CentristTrimmer(
            IdentifiableIDSet column,
            BlockScoreFunction scoreFunc
    ) {
    
        this(
                column,
                scoreFunc,
                new MaxDropFinder<>(
                    new GreaterThanConstraint(BigDecimal.ZERO),
                    false,
                    false
                ),
                new ZeroThreshold(),
                null
        );
    }

    @Override
    public void trim(SignatureBlocks sig, SignatureBlocksConsumer consumer) {

        List<IdentifiableDouble> elements = new ArrayList<>();
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            final int[] block = sig.get(iBlock);
            BigDecimal score = _scoreFunc.score(block, _columnId);
            elements.add(new IdentifiableDouble(iBlock, score));
        }
        Collections.sort(elements, new DoubleValueDescSort());
        int dropIndex = _dropFinder.getPruneIndex(elements);
        if (dropIndex > 0) {
            if (elements.get(0).value() > 0) {
                consumer.consume(new CentristSignature(sig, elements, dropIndex));
            }
        }
    }

    public IDSet trimmedBlocks(SignatureBlocks sig) {

        List<IdentifiableDouble> elements = new ArrayList<>();
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            final int[] block = sig.get(iBlock);
            BigDecimal score = _scoreFunc.score(block, _columnId);
            elements.add(new IdentifiableDouble(iBlock, score));
        }
        Collections.sort(elements, new DoubleValueDescSort());
        int dropIndex = _dropFinder.getPruneIndex(elements);
        
        HashIDSet result = new HashIDSet();
        for (int iEl = 0; iEl < dropIndex; iEl++) {
            result.add(elements.get(iEl).id());
        }
        return result;
    }
}
