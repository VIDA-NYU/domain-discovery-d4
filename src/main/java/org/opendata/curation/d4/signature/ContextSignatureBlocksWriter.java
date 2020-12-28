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

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.opendata.core.io.FileSystem;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureBlocksWriter implements ContextSignatureBlocksConsumer {

    private final File _file;
    private int _openCount = 0;
    private PrintWriter _out = null;
    
    public ContextSignatureBlocksWriter(File file) {
        
        _file = file;
    }
    
    @Override
    public synchronized void close() {

        _openCount--;
        if (_openCount == 0) {
            _out.close();
            _out = null;
        }
    }

    @Override
    public void consume(int nodeId, BigDecimal sim, List<ContextSignatureBlock> blocks) {

        StringBuilder line = new StringBuilder()
                .append(nodeId)
                .append("\t")
                .append(sim.setScale(8, RoundingMode.HALF_DOWN).toPlainString());
        for (ContextSignatureBlock block : blocks) {
            line.append("\t").append(block.termCount());
            for (int iValue = 0; iValue < block.elementCount(); iValue++) {
                ContextSignatureValue value = block.objectAt(iValue);
                line.append(",").append(value.id()).append(":").append(value.overlap());
            }
        }
        synchronized(this) {
            _out.println(line.toString());
        }
    }

    @Override
    public synchronized void open() {
        
        if (_out == null) {
            try {
                _out = FileSystem.openPrintWriter(_file);
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        _openCount++;
    }
}
