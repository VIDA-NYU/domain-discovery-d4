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
package org.opendata.core.object;

import java.io.File;

/**
 * File that is uniquely identified by the name (which is expected to start
 * with an integer).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableFile extends IdentifiableObjectImpl {

    private final File _file;
    
    public IdentifiableFile(File file) {
    
        super(Integer.parseInt(file.getName().substring(0, file.getName().indexOf("."))));
        
        _file = file;
    }
    
    public File file() {
        
        return this.value();
    }
    
    public File value() {
        
        return _file;
    }
}
