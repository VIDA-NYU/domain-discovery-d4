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
package org.opendata.curation.d4.eq;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.sort.DecimalRanking;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;

public class EQClusterCountsPrinter {

	private final EQColumnSimilarityMatrix _matrix;
	
	public EQClusterCountsPrinter(EQIndex eqIndex) {
	
		_matrix = new EQColumnSimilarityMatrix(eqIndex);
	}
	
	public IdentifiableObjectSet<IdentifiableIDSet> run(EQ node) {	
		
		HashObjectSet<IdentifiableColumnPair> edges = new HashObjectSet<>();
		
		List<Integer> columnIds = node.columns().toList();
		for (int iCol = 0; iCol < columnIds.size() - 1; iCol++) {
			int colI = columnIds.get(iCol);
			for (int jCol = iCol + 1; jCol < columnIds.size(); jCol++) {
				int colJ = columnIds.get(jCol);
				double sim = _matrix.getSim(colI, colJ);
				if (sim > 0) {
					edges.add(
							new IdentifiableColumnPair(
									edges.length(),
									new int[] {colI, colJ},
									new BigDecimal(sim)
							)
					);
				}
			}
		}
		List<IdentifiableColumnPair> edgeList = edges.toList();
		Collections.sort(edgeList, new DecimalRanking());
        IDSet prunedEdges = new MaxDropFinder<IdentifiableColumnPair>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                true,
                false
        ).pruneElements(edgeList);
		
		UndirectedConnectedComponents compGen;
		compGen = new UndirectedConnectedComponents(node.columns());
		
		for (int edgeId : prunedEdges) {
			int[] edge = edges.get(edgeId).columns();
			compGen.edge(edge[0], edge[1]);
		}
		
		return compGen.getComponents();
	}
	
	private final static String COMMAND =
			"Usage:\n" +
			"  <eq-file>\n" +
			"  <term-index>\n" +
			"  <output-file>";
	
	private final static Logger LOGGER = Logger
			.getLogger(EQClusterCountsPrinter.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File eqFile = new File(args[0]);
		File termFile = new File(args[1]);
		File outputFile = new File(args[2]);
		
		EQIndex eqIndex = null;
		try {
			eqIndex = new EQIndex(eqFile);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, eqFile.getName(), ex);
			System.exit(-1);
		}
		
		EQClusterCountsPrinter clusterer;
		clusterer = new EQClusterCountsPrinter(eqIndex);
		
		HashIDSet termFilter = new HashIDSet();
		
		List<int[]> clusterCounts = new ArrayList<>();
		for (EQ node : eqIndex) {
			IdentifiableObjectSet<IdentifiableIDSet> clusters;
			clusters = clusterer.run(node);
			int[] counts = new int[] {node.id(), node.columns().length(), clusters.length()};
			clusterCounts.add(counts);
			termFilter.add(node.terms().first());
			System.out.println(node.id() + "\t" + counts[2]);
		}
		Collections.sort(clusterCounts, (final int[] c1, final int[] c2) -> {
			int comp = Integer.compare(c2[2], c1[2]);
			if (comp == 0) {
				comp = Integer.compare(c1[1], c2[1]);
			}
			return comp;
		});
		
		EntitySet terms = null;
		try {
			terms = new EntitySetReader(termFile).readEntities(termFilter);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, termFile.getName(), ex);
			System.exit(-1);
		}
		
		try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
			for (int[] counts : clusterCounts) {
				int nodeId = counts[0];
				String term = terms.get(eqIndex.get(nodeId).terms().first()).name();
				String line = nodeId + "\t" + counts[1] + "\t" + counts[2] + "\t" + term;
				out.println(line);
				System.out.println(line);
			}
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
