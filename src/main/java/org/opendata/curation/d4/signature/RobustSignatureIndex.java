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

import java.util.Iterator;
import java.util.List;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;

/**
 * Memory buffer for signature blocks indexed by their identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class RobustSignatureIndex implements Iterable<RobustSignature>, RobustSignatureConsumer, RobustSignatureStream {

    private final HashObjectSet<RobustSignature> _signatures = new HashObjectSet<>();
    
    public void clear() {
        
        _signatures.clear();
    }
    
    @Override
    public void close() {

    }

    @Override
    public synchronized void consume(RobustSignature sig) {

        _signatures.add(sig);
    }

    public RobustSignature get(int id) {
        
        return _signatures.get(id);
    }
    
    @Override
    public Iterator<RobustSignature> iterator() {

        return _signatures.iterator();
    }
    
    public int length() {
        
        return _signatures.length();
    }

    @Override
    public void open() {

    }

    @Override
    public void stream(RobustSignatureConsumer consumer) {

        consumer.open();
        
        for (RobustSignature sig : _signatures) {
            consumer.consume(sig);
        }
        
        consumer.close();
    }

    public void stream(RobustSignatureConsumer consumer, IDSet filter) {

        consumer.open();
        
        for (int sigId : filter) {
            if (_signatures.contains(sigId)) {
                consumer.consume(_signatures.get(sigId));
            }
        }
        
        consumer.close();
    }
    
    public List<RobustSignature> toList() {
        
        return _signatures.toList();
    }
}
