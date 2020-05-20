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

import java.util.HashMap;
import org.opendata.db.eq.EQIndex;

/**
 * Dispatcher of signature blocks to columns that contain the given signature.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnDispatcher implements SignatureBlocksConsumer {
    
    private final HashMap<Integer, SignatureBlocksConsumer> _columns;
    private final EQIndex _eqIndex;

    public ColumnDispatcher(EQIndex eqIndex) {

        _eqIndex = eqIndex;
        _columns = new HashMap<>();
    }

    public void add(int columnId, SignatureBlocksConsumer consumer) {

        _columns.put(columnId, consumer);
    }

    @Override
    public void close() {

        for (SignatureBlocksConsumer consumer : _columns.values()) {
            consumer.close();
        }
   }

    @Override
    public void consume(SignatureBlocks sig) {

        for (int columnId : _eqIndex.get(sig.id()).columns()) {
            if (_columns.containsKey(columnId)) {
                _columns.get(columnId).consume(sig);
            }
        }
    }

    @Override
    public void open() {

        for (SignatureBlocksConsumer consumer : _columns.values()) {
            consumer.open();
        }
    }

    public int size() {

        return _columns.size();
    }
}
