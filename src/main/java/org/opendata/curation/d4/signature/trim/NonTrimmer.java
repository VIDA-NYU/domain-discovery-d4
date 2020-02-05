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
import org.opendata.core.constraint.ZeroThreshold;
import org.opendata.core.set.IDSet;

/**
 * Signature trimmer that simply returns the complete list of signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class NonTrimmer extends SignatureTrimmer {

    public NonTrimmer(
            IDSet column,
            Threshold nonEmptyConstraint,
            SignatureBlocksConsumer consumer
    ) {
        super(column, nonEmptyConstraint, consumer);
    }
    
    public NonTrimmer(IDSet column, SignatureBlocksConsumer consumer) {
        
        this(column, new ZeroThreshold(), consumer);
    }
    
    @Override
    public void trim(SignatureBlocks sig, SignatureBlocksConsumer consumer) {

        consumer.consume(sig);
    }
}
