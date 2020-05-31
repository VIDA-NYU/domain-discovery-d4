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

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.EntitySetReader;
import org.opendata.curation.d4.signature.ContextSignatureGenerator;
import org.opendata.curation.d4.signature.hierarchy.HierarchicalSignature;
import org.opendata.curation.d4.signature.hierarchy.HierarchicalSignatureGenerator;
import org.opendata.curation.d4.signature.hierarchy.SignatureHierarchyTextPrinter;
import org.opendata.curation.d4.signature.similarity.ColumnSetJaccard;
import org.opendata.curation.d4.signature.similarity.ColumnSetLogJaccard;
import org.opendata.curation.d4.signature.similarity.NodeSimilarityFunction;
import org.opendata.db.eq.EQIndex;

public class ContextSignatureHierarchyPrinter {

	private final static String COMMAND =
			"Usage:\n" +
			"  <eq-file>\n" +
			"  <term-file>\n" +
			"  <use-log> [true | false]\n" +
			"  <max-depth>\n" +
			"  <node-id>";
	
	private final static Logger LOGGER = Logger
			.getLogger(ContextSignatureHierarchyPrinter.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 5) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File eqFile = new File(args[0]);
		File termFile = new File(args[1]);
		boolean useLog = Boolean.parseBoolean(args[2]);
		int maxDepth = Integer.parseInt(args[3]);
		int nodeId = Integer.parseInt(args[4]);
		
		NodeSimilarityFunction simFunc;
		if (useLog) {
			simFunc = new ColumnSetLogJaccard();
		} else {
			simFunc = new ColumnSetJaccard();
		}
		
		EQIndex eqIndex = null;
		try {
			eqIndex = new EQIndex(eqFile);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "READ NODES", ex);
			System.exit(-1);
		}

		ContextSignatureGenerator signatures = null;
		signatures = new ContextSignatureGenerator(
				eqIndex.nodes(),
				simFunc,
				true
		);

		HierarchicalSignature sig;
		sig = new HierarchicalSignatureGenerator(signatures)
			.getSignature(nodeId);
		
		try (PrintWriter out = new PrintWriter(System.out)) {
			new SignatureHierarchyTextPrinter(eqIndex, new EntitySetReader(termFile))
				.print(sig, maxDepth, out);
		} catch (java.lang.RuntimeException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
