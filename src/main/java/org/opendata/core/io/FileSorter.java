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
import java.util.Comparator;
import java.util.HashMap;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class FileSorter implements Comparator<File> {

    private final HashMap<String, String[]> _fileMapping;
    
    public FileSorter(HashMap<String, String[]> fileMapping) {
        
        _fileMapping = fileMapping;
    }
    
    @Override
    public int compare(File f1, File f2) {
        return Integer.compare(
            Integer.parseInt(_fileMapping.get(f1.getName())[1]),
            Integer.parseInt(_fileMapping.get(f2.getName())[1])
        );
    }    
}
