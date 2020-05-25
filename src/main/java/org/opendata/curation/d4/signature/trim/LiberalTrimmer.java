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
import org.opendata.curation.d4.signature.SignatureBlocksImpl;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.constraint.ZeroThreshold;
import org.opendata.core.object.filter.AnyObjectFilter;

/**
 * Liberal signature blocks trimmer. The liberal trimmer prunes all
 * blocks starting from the block with the most elements. Only if the first
 * block is the largest block it will not be pruned.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LiberalTrimmer extends SignatureTrimmer {

    private final int[] _nodeSizes;
    
    public LiberalTrimmer(
            int[] nodeSizes,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        super(new AnyObjectFilter<Integer>(), nonEmptyConstraint, consumer);
        
        _nodeSizes = nodeSizes;
    }
    
    public LiberalTrimmer(int[] nodeSizes, SignatureBlocksConsumer consumer) {
        
        this(nodeSizes, new ZeroThreshold(), consumer);
    }

    @Override
    public void trim(SignatureBlocks sig, SignatureBlocksConsumer consumer) {

        int index = 0;
        int maxIndex = -1;
        int maxSize = -1;
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            int size = 0;
            for (int nodeId : sig.get(iBlock)) {
                size += _nodeSizes[nodeId];
            }
            if (size > maxSize) {
                maxSize = size;
                maxIndex = index;
            }
            index++;
        }
        int[][] blocks = new int[Math.max(1, maxIndex)][];
        for (int iBlock = 0; iBlock < blocks.length; iBlock++) {
            blocks[iBlock] = sig.get(iBlock);
        }
        consumer.consume(new SignatureBlocksImpl(sig.id(), sig.maxSim(), blocks));
    }
}
