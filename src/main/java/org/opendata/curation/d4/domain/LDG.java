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
package org.opendata.curation.d4.domain;

import java.io.File;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.curation.d4.domain.graph.GraphFileWriter;
import org.opendata.db.eq.EQIndex;

/**
 * Generator for local domains using HEX edge files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LDG {
                   
    public void run(
            EQIndex nodes,
            ExpandedColumnIndex columnIndex,
            File graphFile,
            int threads,
            String baseName,
            File runDir
    ) throws java.io.IOException {

        File graphDir = FileSystem.joinPath(runDir, "edges." + baseName);
        FileSystem.createFolder(graphDir);

        // Write graph files
        System.out.println("WRITE EDGE FILES @ " + new Date());
        new GraphFileWriter().run(graphFile, graphDir, true);
        
        // Generate undirected graph domains
        File locDomSingleFile = FileSystem.joinPath(
                runDir,
                "local-domains.Single." + baseName + ".txt.gz"
        );
        new LocalDomainGenerator().run(
                nodes,
                columnIndex,
                graphDir,
                EdgeType.Single,
                threads,
                new DomainWriter(locDomSingleFile)
        );
        
        // Generate directed graph domains
        File locDomDirectedFile = FileSystem.joinPath(
                runDir,
                "local-domains.Directed." + baseName + ".txt.gz"
        );
        new LocalDomainGenerator().run(
                nodes,
                columnIndex,
                graphDir,
                EdgeType.Directed,
                1,
                new DomainWriter(locDomDirectedFile)
        );
       
        // Remove graph files dir
        FileUtils.deleteDirectory(graphDir);
    }
    
    private static final String ARG_COLUMNS = "columns";
    private static final String ARG_THREADS = "threads";
    
    private static final String[] ARGS = {
        ARG_COLUMNS,
        ARG_THREADS
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_COLUMNS + "=<column-list-file> [default: null]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 3]\n" +
            "  <eq-file>\n" +
            "  <columns-file>\n" +
            "  <graph-file>\n" +
            "  <base-name>\n" +
            "  <run-dir>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LDG.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Local Domain Generator (Single & Directed) - Version (" + Constants.VERSION + ")\n");

        if (args.length < 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 5);
        File eqFile = new File(params.fixedArg(0));
        File columnFile = new File(params.fixedArg(1));
        File graphFile = new File(params.fixedArg(2));
        String baseName = params.fixedArg(3);
        File runDir = new File(params.fixedArg(4));

        int threads = params.getAsInt(ARG_THREADS, 3);
                
        File columnsFile = null;
        if (params.has(ARG_COLUMNS)) {
            columnsFile = new File(params.get(ARG_COLUMNS));
        }
        
        FileSystem.createParentFolder(runDir);
        
        try {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            // Read the list of column identifier if a columns file was given
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            if (columnsFile != null) {
                 new ExpandedColumnReader(columnFile)
                         .stream(columnIndex, new HashIDSet(columnsFile));
            } else {
                 new ExpandedColumnReader(columnFile).stream(columnIndex);
            }
            new LDG().run(
                    nodeIndex,
                    columnIndex,
                    graphFile,
                    threads,
                    baseName,
                    runDir
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
