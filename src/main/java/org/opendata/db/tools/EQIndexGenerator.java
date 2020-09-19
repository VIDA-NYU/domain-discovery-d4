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
package org.opendata.db.tools;

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Constants;
import org.opendata.core.io.FileSystem;
import org.opendata.db.eq.CompressedTermIndexGenerator;
import org.opendata.db.term.TermIndexReader;

/**
 * Create an equivalence class file from a given term index file.
 * 
 * The column threshold allows to ignore terms that do not occur in many
 * different columns (i.e., ignore all terms that occur in less columns that
 * the given threshold).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQIndexGenerator {
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <term-index-file>\n" +
            "  <output-file>";
            
    private static final Logger LOGGER = Logger
            .getLogger(EQIndexGenerator.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Equivalence Class Index Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);     
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new TermIndexReader(inputFile)
                    .read(new CompressedTermIndexGenerator(out, true));
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, outputFile.getName(), ex);
            System.exit(-1);
        }
    }
}
