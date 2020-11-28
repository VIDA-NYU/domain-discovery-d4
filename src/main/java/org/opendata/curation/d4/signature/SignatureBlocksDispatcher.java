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

import java.util.ArrayList;
import java.util.List;

/**
 * The dispatcher manages a set of consumers. Each signature that is handled by
 * the dispatcher is forwarded to each of the managed consumers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksDispatcher implements SignatureBlocksConsumer {
    
    private final List<SignatureBlocksConsumer> _consumers;

    public SignatureBlocksDispatcher(List<SignatureBlocksConsumer> consumers) {

        _consumers = consumers;
    }

    public SignatureBlocksDispatcher() {
        
        this(new ArrayList<>());
    }
    
    public void add(SignatureBlocksConsumer consumer) {
        
        _consumers.add(consumer);
    }
    
    @Override
    public void close() {

        for (SignatureBlocksConsumer consumer : _consumers) {
            consumer.close();
        }
   }

    @Override
    public void consume(SignatureBlocks sig) {

        for (SignatureBlocksConsumer consumer : _consumers) {
            consumer.consume(sig);
        }
    }

    @Override
    public boolean isDone() {
        
        for (SignatureBlocksConsumer consumer : _consumers) {
            if (consumer.isDone()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void open() {

        for (SignatureBlocksConsumer consumer : _consumers) {
            consumer.open();
        }
    }
}