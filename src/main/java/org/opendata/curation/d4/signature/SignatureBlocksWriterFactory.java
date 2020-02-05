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
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.core.io.FileSystem;

/**
 * Signature blocks writer factory.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksWriterFactory implements SignatureBlocksConsumerFactory {

    private int _count = 0;
    private final File _file;
    private SignatureBlocksConsumer _globalConsumer = null;
    private final boolean _outputToDir;
    
    public SignatureBlocksWriterFactory(File file, boolean outputToDir) {
        
        _file = file;
        _outputToDir = outputToDir;
        
        if (outputToDir) {
             FileSystem.createFolder(file);
        } else {
            FileSystem.createParentFolder(file);
        }        
    }
    
    @Override
    public SignatureBlocksConsumer getConsumer(int[] nodeSizes) {

        if (_outputToDir) {
            String filename = "signature-blocks." + (_count++) + ".txt.gz";
            File outputFile = FileSystem.joinPath(_file, filename);
            return new LiberalTrimmer(
                        nodeSizes,
                        new SignatureBlocksWriter(outputFile)
                );
        } else {
            if (_globalConsumer == null) {
                _globalConsumer = new LiberalTrimmer(
                    nodeSizes,
                    new SignatureBlocksWriter(_file)
                );
            }
            return _globalConsumer;
        }
    }
    
    @Override
    public SignatureBlocksIndex signatures() throws java.io.IOException {

        SignatureBlocksIndex buffer = new SignatureBlocksIndex();
        new SignatureBlocksReader(_file).stream(_globalConsumer);
        return buffer;
    }
}
