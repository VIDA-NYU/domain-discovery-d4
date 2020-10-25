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
import java.util.ArrayList;
import java.util.List;
import org.opendata.core.io.FileSystem;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;

/**
 * Signature blocks writer factory. Returns consumer that are open. Since this
 * factory returns the same consumer multiple times if writing to a single file
 * the calling method should not close the consumer but call the close method
 * of this class instead at the end of processing.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksWriterFactory implements SignatureBlocksConsumerFactory {

    private int _count = 0;
    private final File _file;
    private SignatureBlocksConsumer _globalConsumer = null;
    private final SignatureTrimmerFactory _trimmerFactory;
    private final boolean _outputToDir;
    private List<SignatureBlocksConsumer> _openConsumer = null;
    
    public SignatureBlocksWriterFactory(
            File file,
            SignatureTrimmerFactory trimmerFactory,
            boolean outputToDir
    ) {
        
        _file = file;
        _trimmerFactory = trimmerFactory;
        _outputToDir = outputToDir;
        
        _openConsumer = new ArrayList<>();
        
        if (outputToDir) {
             FileSystem.createFolder(file);
        } else {
            FileSystem.createParentFolder(file);
        }        
    }

    @Override
    public void close() {
        
        for (SignatureBlocksConsumer consumer : _openConsumer) {
            consumer.close();
        }
        _globalConsumer = null;
        _openConsumer = null;
    }
    
    @Override
    public SignatureBlocksConsumer getConsumer() {

        if (_outputToDir) {
            String filename = "signature-blocks." + (_count++) + ".txt.gz";
            File outputFile = FileSystem.joinPath(_file, filename);
            SignatureBlocksConsumer trimmer = _trimmerFactory.getTrimmer(
                    new SignatureBlocksWriter(outputFile)
            );
            trimmer.open();
            _openConsumer.add(trimmer);
            return trimmer;
        } else {
            if (_globalConsumer == null) {
                _globalConsumer = _trimmerFactory.getTrimmer(
                    new SignatureBlocksWriter(_file)
                );
                _globalConsumer.open();
                _openConsumer.add(_globalConsumer);
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
