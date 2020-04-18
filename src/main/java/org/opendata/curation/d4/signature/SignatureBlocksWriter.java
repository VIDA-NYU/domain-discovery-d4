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
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.core.util.StringHelper;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksWriter implements SignatureBlocksConsumer {

    private final File _file;
    private PrintWriter _out = null;
    
    public SignatureBlocksWriter(File file) {
        
        _file = file;
    }
    
    @Override
    public void close() {

        _out.close();
        _out = null;
    }

    @Override
    public void consume(SignatureBlocks sig) {

        String line = sig.id() + "\t" + new FormatedBigDecimal(sig.maxSim()).toString();
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            line += "\t" + StringHelper.joinIntegers(sig.get(iBlock));
        }
        synchronized(this) {
            _out.println(line);
        }
    }

    @Override
    public void open() {
        
        try {
            _out = FileSystem.openPrintWriter(_file);
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public void write(SignatureBlocksIndex signatures) {
        
        signatures.stream(this);
    }
}
