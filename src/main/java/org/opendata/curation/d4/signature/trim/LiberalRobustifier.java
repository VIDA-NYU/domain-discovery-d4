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

import java.util.List;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlock;

/**
 * Liberal signature blocks trimmer. The liberal trimmer prunes all
 * blocks starting from the block with the most elements. Only if the first
 * block is the largest block it will not be pruned.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LiberalRobustifier extends SignatureRobustifier {

    private final int[] _nodeSizes;
    
    public LiberalRobustifier(int[] nodeSizes, SignatureBlocksConsumer consumer) {
        super(consumer);
        
        _nodeSizes = nodeSizes;
    }

    @Override
    public void consume(int nodeId, List<SignatureBlock> blocks) {

        int index = 0;
        int maxIndex = -1;
        int maxSize = -1;
        for (SignatureBlock block : blocks) {
            int size = 0;
            for (int memberId : block.elements()) {
                size += _nodeSizes[memberId];
            }
            if (size > maxSize) {
                maxSize = size;
                maxIndex = index;
            }
            index++;
        }
        this.push(nodeId, blocks, Math.max(1, maxIndex));
    }
}
