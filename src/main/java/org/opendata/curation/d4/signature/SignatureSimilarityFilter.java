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

import org.opendata.core.constraint.Threshold;

/**
 * Filter signature blocks based on the similarity of their first entry.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureSimilarityFilter implements SignatureBlocksConsumer {

    private final SignatureBlocksConsumer _consumer;
    private final Threshold _threshold;
    
    public SignatureSimilarityFilter(Threshold threshold, SignatureBlocksConsumer consumer) {
        
        _threshold = threshold;
        _consumer = consumer;
    }
    
    @Override
    public void close() {

        _consumer.close();
    }

    @Override
    public void consume(SignatureBlocks sig) {

        if (_threshold.isSatisfied(sig.maxSim())) {
            _consumer.consume(sig);
        }
    }

    @Override
    public void open() {

        _consumer.open();
    }
}
