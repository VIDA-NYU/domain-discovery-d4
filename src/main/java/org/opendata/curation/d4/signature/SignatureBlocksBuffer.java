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
import java.util.Iterator;
import java.util.List;

/**
 * Memory buffer for signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksBuffer implements Iterable<SignatureBlocks>, SignatureBlocksConsumer, SignatureBlocksStream {

    private final List<SignatureBlocks> _signatures = new ArrayList<>();
    private final String _source;
    
    public SignatureBlocksBuffer(String source) {
        
        _source = source;
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(SignatureBlocks sig) {

        _signatures.add(sig);
    }

    public SignatureBlocks get(int index) {
        
        return _signatures.get(index);
    }

    @Override
    public boolean isDone() {
        
        return false;
    }
    
    @Override
    public Iterator<SignatureBlocks> iterator() {

        return _signatures.iterator();
    }

    @Override
    public void open() {

        _signatures.clear();
    }
    
    public int size() {
        
        return _signatures.size();
    }

    @Override
    public void stream(SignatureBlocksConsumer consumer) {

        consumer.open();
        
        for (SignatureBlocks sig : _signatures) {
            consumer.consume(sig);
        }
        
        consumer.close();
    }

    @Override
    public String source() {

        return _source;
    }
}
