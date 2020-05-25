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

import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.constraint.ZeroThreshold;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.db.eq.EQIndex;

/**
 * Signature trimmer factory that allows for CENTRIST trimmer
 * threshold constraints.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class ConstraintTrimmerFactory {

	private final Threshold _trimmerThreshold;
	private final TrimmerType _trimmerType;
	private final int[] _nodeSizes;
	
	public ConstraintTrimmerFactory(String trimmer, EQIndex eqIndex) {
		
        if (trimmer.contains(":")) {
            String[] tokens = trimmer.split(":");
            _trimmerType = TrimmerType.valueOf(tokens[0]);
            _trimmerThreshold = Threshold.getConstraint(tokens[1]);
        } else {
            _trimmerType = TrimmerType.valueOf(trimmer);
            _trimmerThreshold = new GreaterThanConstraint(BigDecimal.ZERO);
        }
		_nodeSizes = eqIndex.nodeSizes();
	}
	
	public SignatureTrimmer getTrimmer(IDSet nodes, SignatureBlocksConsumer consumer) {
		
        switch (_trimmerType) {
            case CONSERVATIVE:
                return new ConservativeTrimmer(
                		nodes,
                        consumer
                );
            case CENTRIST:
                return new CentristTrimmer(
                		nodes,
                        _nodeSizes,
                        new PrecisionScore(),
                        new MaxDropFinder<>(
                                _trimmerThreshold,
                                false,
                                false
                        ),
                        new ZeroThreshold(),
                        consumer
                );
            default:
                return new NonTrimmer(
                		nodes,
                        consumer
                );
        }
	}
}
