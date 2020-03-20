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
package org.opendata.core.query.json;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple path expression. A JPath is simply a list of Json element keys.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JPath implements Iterable<String> {
    
    private final String[] _path;
    
    public JPath(String path) {

        String trimmedPath = path;
        while (trimmedPath.startsWith("/")) {
            trimmedPath = trimmedPath.substring(1);
        }
        while (trimmedPath.endsWith("/")) {
            trimmedPath = trimmedPath.substring(0, trimmedPath.length() - 1);
        }
        _path = trimmedPath.split("/");
    }
    
    public String get(int index) {
        
        return _path[index];
    }
    
    public int size() {
        
        return _path.length;
    }

    @Override
    public Iterator<String> iterator() {

        return Arrays.asList(_path).iterator();
    }
}
