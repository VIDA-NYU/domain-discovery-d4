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
package org.opendata.curation.d4.explore;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.opendata.core.io.FileSystem;

/**
 * Read equivalence class summary information. Expects an input file with
 * five columns:
 * 
 * 1) EQ identifier
 * 2) Number of columns the EQ occurs in
 * 3) Number of columns the EQ was added to be expansion
 * 4) Number of local domains the EQ occurs in
 * 5) Number of strong domains the EQ occurs in
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQSummaryReader {
    
    private final File _file;
    
    public EQSummaryReader(File file) {
        
        _file = file;
    }
    
    public List<EQSummary> read() throws java.io.IOException {
        
        List<EQSummary> nodes = new ArrayList<>();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int nodeId = Integer.parseInt(tokens[0]);
                String[] properties = new String[tokens.length - 1];
                for (int iProp = 1; iProp < tokens.length; iProp++) {
                    properties[iProp - 1] = tokens[iProp];
                }
                nodes.add(new EQSummary(nodeId, properties));
            }
        }
        
        return nodes;
    }
}
