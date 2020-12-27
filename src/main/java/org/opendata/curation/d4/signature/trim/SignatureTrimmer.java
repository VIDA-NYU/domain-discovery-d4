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
import java.util.List;
import org.opendata.core.object.ObjectFilter;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.SignatureBlock;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;

/**
 * Base class for signature trimmer. The trimmer is used to generate robust
 * signatures for individual columns. The class maintains a list of column
 * elements in order to decide which signatures to trim and which to ignore.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class SignatureTrimmer implements SignatureBlocksConsumer {
    
    private final RobustSignatureConsumer _consumer;
    private final ObjectFilter<Integer> _filter;
        
    /**
     * Initialize the consumer for trimmed signatures and the column filter.
     * The filter is used to ensure that only the signature for column elements
     * are being trimmed.
     * 
     * @param consumer 
     * @param filter 
     */
    public SignatureTrimmer(
            ObjectFilter<Integer> filter,
            RobustSignatureConsumer consumer
    ) {
        _filter = filter;
        _consumer = consumer;
    }
    
    @Override
    public void close() {

        _consumer.close();
    }
    
    @Override
    public void consume(int id, BigDecimal sim, List<SignatureBlock> blocks) {

        if ((_filter.contains(id)) && (!blocks.isEmpty())) {
            this.trim(id, blocks, _consumer);
        }
    }
    
    @Override
    public void open() {

        _consumer.open();
    }    
    
    /**
     * Trim the given signature and pass the result to the given consumer.
     * 
     * @param id
     * @param blocks
     * @param consumer 
     */
    public abstract void trim(int id, List<SignatureBlock> blocks, RobustSignatureConsumer consumer);
}
