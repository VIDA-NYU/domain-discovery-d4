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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.Threshold;
import org.opendata.curation.d4.Constants;
import org.opendata.core.io.FileListReader;

/**
 * Create a term index file. The output file is tab-delimited and contains three
 * columns: (1) the term identifier, (2) the term, and a comma-separated list of
 * column identifier:count pairs.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexGenerator {

    private final static String COMMAND =
	    "Usage:\n" +
	    "  <column-file-or-dir>\n" +
            "  <text-threshold>\n" +
	    "  <mem-buffer-size>\n" +
	    "  <output-file>";
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Term Index Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputDirectory = new File(args[0]);
        Threshold textThreshold = Threshold.getConstraint(args[1]);
        int bufferSize = Integer.parseInt(args[2]);
        File outputFile = new File(args[3]);
        
        try {
            new org.opendata.db.term.TermIndexGenerator().run(
                    new FileListReader(".txt").listFiles(inputDirectory),
                    textThreshold,
                    bufferSize,
                    true,
                    outputFile
            );
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
