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
package org.opendata.db.column;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.FileSystem;
import org.opendata.core.io.SynchronizedWriter;
import org.opendata.core.object.IdentifiableArray;
import org.opendata.db.Database;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.tools.OverlapComputer;

/**
 * Compute pairwise overlap between all columns in a database.
 * 
 * @author Heiko Mueller
 *
 */
public class ColumnOverlapComputer {

    public void run(
    		EQIndex eqIndex,
    		int threads,
			File outputFile
	) throws java.lang.InterruptedException, java.io.IOException {
    
    	List<IdentifiableArray> columns = new ArrayList<>();
    	for (Column col : new Database(eqIndex).columns()) {
    		columns.add(new IdentifiableArray(col.id(), col.toArray()));
    	}
    	
    	try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
    		new OverlapComputer().run(
					columns,
					eqIndex.nodeSizes(),
					threads,
					new SynchronizedWriter(out)
			);
    	}
    }
    
    private final static String COMMAND =
    		"Usage:\n" +
			"  <eq-file>\n" +
			"  <threads>\n" +
			"  <output-file>";
    
    public final static Logger LOGGER = Logger
    		.getLogger(ColumnOverlapComputer.class.getName());
    
    public static void main(String[] args) {
    	
    	if (args.length != 3) {
    		System.out.println(COMMAND);
    		System.exit(-1);
    	}
    	
    	File eqFile = new File(args[0]);
    	int threads = Integer.parseInt(args[1]);
    	File outputFile = new File(args[2]);
    	
    	try {
    		new ColumnOverlapComputer().run(new EQIndex(eqFile), threads, outputFile);
    	} catch (java.lang.InterruptedException | java.io.IOException ex) {
    		LOGGER.log(Level.SEVERE, "RUN", ex);
    		System.exit(-1);
    	}
    }
}
