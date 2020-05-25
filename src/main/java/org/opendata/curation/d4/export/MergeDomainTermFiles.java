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
package org.opendata.curation.d4.export;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.FileSystem;
import org.opendata.db.term.BufferedTermIndexReader;

/**
 * Merge term files form multiple domains. Keeps only those terms that
 * occur in more than one domain. For each term the number of columns
 * in the domain it occurs in ins maintained.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class MergeDomainTermFiles {

	private void insert(LinkedList<BufferedTermIndexReader> datasources, BufferedTermIndexReader reader) {
	
		for (int iPos = 0; iPos < datasources.size(); iPos++) {
			if (reader.compareTo(datasources.get(iPos)) <= 0) {
				datasources.add(iPos, reader);
				return;
			}
		}
		datasources.add(reader);
	}
	
	public void run(File baseDir, PrintWriter out) throws java.io.IOException {
		
		LinkedList<BufferedTermIndexReader> datasources = new LinkedList<>();
		for (File dir : baseDir.listFiles()) {
			if (dir.isDirectory()) {
				File termFile = FileSystem.joinPath(dir, "term-index.txt.gz");
				if (termFile.exists()) {
					BufferedTermIndexReader reader = new BufferedTermIndexReader(termFile);
					if (reader.hasNext()) {
						datasources.add(reader);
					} else {
						System.out.println("EMPTY READER " + reader.directory());
					}
				} else {
					System.out.println("SKIP " + dir.getName());
				}
			}
		}
		
		System.out.println("MERGE " + datasources.size() + " DATA SOURCES");
		
		Collections.sort(datasources);
		
		int writeCount = 0;
		while (datasources.size() > 1) {
			List<BufferedTermIndexReader> candidates = new ArrayList<>();
			BufferedTermIndexReader reader = datasources.pop();
			candidates.add(reader);
			String key = reader.peek().name();
			while (!datasources.isEmpty()) {
				if (datasources.get(0).peek().name().equals(key)) {
					candidates.add(datasources.pop());
				} else {
					break;
				}
			}
			if (candidates.size() > 1) {
				String line = key + "\t" + reader.peek().columns().length();
				for (int iCand = 1; iCand < candidates.size(); iCand++) {
					line += "," + candidates.get(iCand).peek().columns().length();
				}
				out.println(line);
				writeCount++;
			}
			for (BufferedTermIndexReader candReader : candidates) {
				candReader.next();
				if (candReader.hasNext()) {
					this.insert(datasources, candReader);
				} else {
					System.out.println("CLOSE " + reader.directory());
					candReader.close();
				}
			}
		}
		if (datasources.size() == 1) {
			datasources.get(0).close();
		}
		
		System.out.println("TOTAL TERMS WRITTEN " + writeCount);
	}
	
	private final static String COMMAND =
			"Usage:\n" +
			"  <base-dir>\n" +
			"  <output-file>";
	
	private final static Logger LOGGER = Logger
			.getLogger(MergeDomainTermFiles.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 2) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File baseDir = new File(args[0]);
		File outputFile = new File(args[1]);
		
		try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
			new MergeDomainTermFiles().run(baseDir, out);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
