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
import org.opendata.core.constraint.Threshold;
import org.opendata.core.object.filter.ObjectFilter;

/**
 * Base class for signature trimmer. The trimmer is used to generate robust
 * signatures for individual columns. The class maintains a list of column
 * elements in order to decide which signatures to trim and which to ignore.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class SignatureTrimmer implements SignatureBlocksConsumer {
    
    /**
     * Global variables for trimmer types
     */
    public final static String CENTRIST = "CENTRIST";
    public final static String COLSUPP = "COLSUPP";
    public final static String CONSERVATIVE = "CONSERVATIVE";
    public final static String LIBERAL = "LIBERAL";
    
    private final SignatureBlocksConsumer _consumer;
    private final ObjectFilter<Integer> _filter;
    private final Threshold _nonEmptyConstraint;
        
    /**
     * Initialize the consumer for trimmed signatures, the column filter, and
     * the empty signature constraint. The filter is used to ensure that only
     * the signature for column elements are being trimmed. The constraint
     * determines which signatures are being empty (based on the value of the
     * most similar node in the signature).
     * 
     * @param consumer 
     * @param filter 
     * @param nonEmptyConstraint 
     */
    public SignatureTrimmer(
            ObjectFilter<Integer> filter,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        _filter = filter;
        _nonEmptyConstraint = nonEmptyConstraint;
        _consumer = consumer;
    }
    
    @Override
    public void close() {

        _consumer.close();
    }
    
    @Override
    public void consume(SignatureBlocks sig) {

        if ((_filter.contains(sig.id())) && (!sig.isEmpty())) {
            if (_nonEmptyConstraint.isSatisfied(sig.maxSim())) {
                this.trim(sig, _consumer);
            }
        }
    }

    @Override
    public boolean isDone() {
        
        return _consumer.isDone();
    }
    
    @Override
    public void open() {

        _consumer.open();
    }    
    
    /**
     * Trim the given signature and pass the result to the given consumer.
     * 
     * @param sig
     * @param consumer 
     */
    public abstract void trim(SignatureBlocks sig, SignatureBlocksConsumer consumer);
}
