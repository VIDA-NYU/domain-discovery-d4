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
package org.opendata.curation.d4.signature.sketch;

import org.opendata.curation.d4.signature.RobustSignatureConsumer;

/**
 * Factory pattern for generating signature blocks consumers for sketches.
 * Each type of sketch generator class will implement their own factory.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface SignatureBlocksSketchFactory {
    
    /**
     * Get a signature blocks sketch consumer. The returned consumer will pass
     * the modified signatures to the consumer that is given as the argument.
     * 
     * @param consumer
     * @return 
     */
    public RobustSignatureConsumer getConsumer(RobustSignatureConsumer consumer);
    
    /**
     * Get documentation string for the signature blocks sketches that are
     * created by the consumer that are returned by this factory.
     * 
     * @return 
     */
    public String toDocString();
}
