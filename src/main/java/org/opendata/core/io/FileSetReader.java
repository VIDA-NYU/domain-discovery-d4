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
package org.opendata.core.io;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Base class for readers that iterate over one or multiple file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class FileSetReader implements Iterable<File>{
    
    private final List<File> _files;
    
    public FileSetReader(File file, boolean verbose) {
        
        _files = new ArrayList<>();
        if (file.isDirectory()) {
            FileSystem.listFilesRecursive(file, _files);
        } else {
            _files.add(file);
        }
        
        if (verbose) {
            System.out.println("FILE(S):");
            for (File sigFile : _files) {
                System.out.println(sigFile.getAbsolutePath());
            }
        }
    }

    @Override
    public Iterator<File> iterator() {
        
        return _files.iterator();
    }
}
