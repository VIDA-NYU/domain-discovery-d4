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
package org.opendata.db.eq;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Create a set of node identifier files for a given equivalence class file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQNodeSetSplitter {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <number-of-files>\n" +
            "  <output-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(EQNodeSetSplitter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int numberOfFiles = Integer.parseInt(args[1]);
        File outputDir = new File(args[2]);
        
        try {
            new EQIndex(eqFile)
                    .splitNodes(numberOfFiles, "nodes", outputDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
