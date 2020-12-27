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

import org.opendata.curation.d4.signature.MultiBlockSignature;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.IDSet;
import org.opendata.core.sort.DoubleValueDescSort;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.SignatureBlock;

/**
 * Centrist signature blocks trimmer. The centrist trimmer uses a scoring
 * function to assign weights to each signature block based on overlap with
 * elements in the given column. It uses a drop finder to then decide which
 * blocks to keep or prune.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CentristTrimmer extends SignatureTrimmer {

    private final MaxDropFinder<IdentifiableDouble> _dropFinder;
    private final BlockScoreFunction _scoreFunc;

    public CentristTrimmer(
            IDSet column,
            BlockScoreFunction scoreFunc,
            MaxDropFinder<IdentifiableDouble> dropFinder,
            RobustSignatureConsumer consumer
    ) {
        super(column, consumer);
        
        _scoreFunc = scoreFunc;
        _dropFinder = dropFinder;
    }

    public CentristTrimmer(
            IDSet column,
            BlockScoreFunction scoreFunc,
            RobustSignatureConsumer consumer
    ) {
    
        this(
                column,
                scoreFunc,
                new MaxDropFinder<>(
                    new GreaterThanConstraint(BigDecimal.ZERO),
                    false,
                    false
                ),
                consumer
        );
    }

    @Override
    public void trim(int id, List<SignatureBlock> blocks, RobustSignatureConsumer consumer) {

        List<IdentifiableDouble> elements = new ArrayList<>();
        for (int iBlock = 0; iBlock < blocks.size(); iBlock++) {
            final SignatureBlock block = blocks.get(iBlock);
            BigDecimal score = _scoreFunc.score(block);
            elements.add(new IdentifiableDouble(iBlock, score));
        }
        Collections.sort(elements, new DoubleValueDescSort());
        int dropIndex = _dropFinder.getPruneIndex(elements);
        List<SignatureBlock> sig = new ArrayList<>();
        for (int iEl = 0; iEl < dropIndex; iEl++) {
            IdentifiableDouble el = elements.get(iEl);
            if (el.value() > 0) {
                sig.add(blocks.get(el.id()));
            } else {
                break;
            }
        }
        if (!sig.isEmpty()) {
            consumer.consume(new MultiBlockSignature(id, sig));
        }
    }
}
