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
package org.opendata.curation.d4.signature;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Worker for generating signature blocks for equivalence classes.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BlockGeneratorTask implements Runnable {
    
    private final SignatureBlocksGenerator _blockGenerator;
    private final SignatureBlocksConsumer _consumer;
    private final ConcurrentLinkedQueue<Integer> _queue;
    private final ContextSignatureGenerator _sigFact;

    public BlockGeneratorTask(
            ConcurrentLinkedQueue<Integer> queue,
            ContextSignatureGenerator sigFact,
            SignatureBlocksGenerator blockGenerator,
            SignatureBlocksConsumer consumer
    ) {

        _queue = queue;
        _sigFact = sigFact;
        _blockGenerator = blockGenerator;
        _consumer = consumer;
    }

    @Override
    public void run() {

        Integer nodeId;
        while ((nodeId = _queue.poll()) != null) {
            List<SignatureBlock> blocks = _blockGenerator
                    .toBlocks(_sigFact.getSignature(nodeId).rankedElements());
            if (!blocks.isEmpty()) {
                _consumer.consume(nodeId, blocks);
            }
        }
    }
}
