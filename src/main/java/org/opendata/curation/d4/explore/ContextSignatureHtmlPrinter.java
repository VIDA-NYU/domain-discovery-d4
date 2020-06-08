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
import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.curation.d4.signature.ContextSignatureGenerator;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.SignatureValue;
import org.opendata.curation.d4.signature.hierarchy.Bucket;
import org.opendata.curation.d4.signature.hierarchy.HierarchicalSignature;
import org.opendata.curation.d4.signature.hierarchy.HierarchicalSignatureGenerator;
import org.opendata.curation.d4.signature.similarity.ColumnSetJaccard;
import org.opendata.curation.d4.signature.similarity.ColumnSetLogJaccard;
import org.opendata.curation.d4.signature.similarity.NodeSimilarityFunction;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.db.eq.EQIndex;

public class ContextSignatureHtmlPrinter {

	public void write(
			EQIndex eqIndex,
			EntitySetReader reader,
			HierarchicalSignature sig,
			SignatureBlocks blocks,
			int maxDepth,
			boolean useLog,
			PrintWriter out
	) throws java.io.IOException {
		
		int[] nodes = sig.nodes();
		int [] mapping = new int[nodes.length];
		
		HashIDSet filter = new HashIDSet();
		for (int iPos = 0; iPos < nodes.length; iPos++) {
			int nodeId = nodes[iPos];
			int termId = eqIndex.get(nodeId).terms().first();
			filter.add(termId);
			mapping[iPos] = termId;
		}
		EntitySet terms = reader.readEntities(filter);
		
		HashIDSet borders = new HashIDSet();
		int border = 0;
		for (int iBlock = 0; iBlock < blocks.size(); iBlock++) {
			border += blocks.get(iBlock).length;
			borders.add(border);
		}
		
		String term = terms.get(mapping[0]).name();
		String sim;
		if (useLog) {
			sim = "Jaccard Similarity (log)";
		} else {
			sim = "Jaccard Similarity";
		}

		out.println("<!DOCTYPE html>");
		out.println("<html>");
		out.println("<meta charset=\"utf-8\">");
		out.println("<title>" + term + " - " + sim + "</title>");
		out.println("<link href=\"./styles.css\" rel=\"stylesheet\">");
		out.println("</html>");
		out.println("</html>");
		out.println("<h1>" + term + "<span class=\"sim\">" + sim + "</span></h1>");
		
		this.write(sig.root(), "", "&nbsp;&nbsp;&nbsp;&nbsp;", 0, maxDepth, terms, mapping, borders, 0, out);
	}
	
	private int write(
			Bucket bucket,
			String indent,
			String extend,
			int depth,
			int maxDepth,
			EntitySet terms,
			int[] nodes,
			HashIDSet borders,
			int nodeCount,
			PrintWriter out
	) {
		
		if ((depth == maxDepth) || (!bucket.hasChildren())) {
			for (int iNode = bucket.startIndex(); iNode < bucket.endIndex(); iNode++) {
				String term = terms.get(nodes[iNode]).name();
				nodeCount++;
				String css = "term";
				if (borders.contains(nodeCount)) {
					if (nodeCount == borders.maxId()) {
						css += " border last";
					} else {
						css += " border";
					}
				}
				out.println("<p class=\"" + css + "\">" + indent + term + "</p>");
			}
		} else {
			if (!bucket.leftChild().isEmpty()) {
				String dropText = String.format("Drop (%d) - Left Child", bucket.drop());
				out.println("<p class=\"drop\">" + indent + dropText + "</p>");
				nodeCount = this.write(
						bucket.leftChild(),
						indent + extend,
						extend,
						depth + 1,
						maxDepth,
						terms,
						nodes,
						borders,
						nodeCount,
						out
					);
			}
			if (!bucket.rightChild().isEmpty()) {
				String dropText = String.format("Drop (%d) - Right Child", bucket.drop());
				out.println("<p class=\"drop\">" + indent + dropText + "</p>");
				nodeCount = this.write(
						bucket.rightChild(),
						indent + extend,
						extend,
						depth + 1,
						maxDepth,
						terms,
						nodes,
						borders,
						nodeCount,
						out
				);
			}
		}
		
		return nodeCount;
	}
	
	private final static String COMMAND =
			"Usage:\n" +
			"  <eq-file>\n" +
			"  <term-file>\n" +
			"  <use-log> [true | false]\n" +
			"  <max-depth>\n" +
			"  <node-id>";
	
	private final static Logger LOGGER = Logger
			.getLogger(ContextSignatureHtmlPrinter.class.getName());
	
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

        MaxDropFinder<SignatureValue> candidateFinder;
        candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                false,
                true
        );
        SignatureBlocks sigBlocks = signatures
                .getSignature(nodeId)
                .toSignatureBlocks(candidateFinder);
        
        SignatureBlocksIndex buffer = new SignatureBlocksIndex();
        new LiberalTrimmer(eqIndex.nodeSizes(), null).trim(sigBlocks, buffer);
        
		HierarchicalSignature hierarchySig;
		hierarchySig = new HierarchicalSignatureGenerator(signatures)
			.getSignature(nodeId);
		
		try (PrintWriter out = new PrintWriter(System.out)) {
			new ContextSignatureHtmlPrinter()
				.write(
						eqIndex,
						new EntitySetReader(termFile),
						hierarchySig,
						buffer.get(nodeId),
						maxDepth,
						useLog,
						out
				);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
