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
package org.opendata.curation.d4.column;

import java.io.File;
import java.util.HashMap;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnWriterFactory implements ExpandedColumnConsumerFactory {

    private int _count = 0;
    private final File _file;
    private ExpandedColumnWriter _globalWriter = null;
    private final boolean _outputToDir;
    
    public ExpandedColumnWriterFactory(File file, boolean outputToDir) {
        
        _file = file;
        _outputToDir = outputToDir;
        
        if (outputToDir) {
             FileSystem.createFolder(file);
        } else {
            FileSystem.createParentFolder(file);
        }        
    }
    
    @Override
    public synchronized ExpandedColumnConsumer getConsumer(HashMap<Integer, HashIDSet> groups) {

        if (_outputToDir) {
            String filename = "expanded-columns." + (_count++) + ".txt.gz";
            File outputFile = FileSystem.joinPath(_file, filename);
            return new ExpandedColumnWriter(outputFile, groups);
        } else {
            if (_globalWriter == null) {
                _globalWriter = new ExpandedColumnWriter(_file, groups);
            }
            return _globalWriter;
        }
    }
}
