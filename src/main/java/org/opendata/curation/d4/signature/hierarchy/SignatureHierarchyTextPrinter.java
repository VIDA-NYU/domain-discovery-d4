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
package org.opendata.curation.d4.signature.hierarchy;

import java.io.PrintWriter;

import org.opendata.core.io.EntitySetReader;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.db.eq.EQIndex;

public class SignatureHierarchyTextPrinter implements SignatureHierarchyPrinter {

	private final EQIndex _eqIndex;
	private final EntitySetReader _reader;
	
	public SignatureHierarchyTextPrinter(EQIndex eqIndex, EntitySetReader reader) {
		
		_eqIndex = eqIndex;
		_reader = reader;
	}
	
	@Override
	public void print(HierarchicalSignature sig, int maxDepth, PrintWriter out) {
		
		int[] nodes = sig.nodes();
		int [] mapping = new int[nodes.length];
		
		HashIDSet filter = new HashIDSet();
		for (int iPos = 0; iPos < nodes.length; iPos++) {
			int nodeId = nodes[iPos];
			int termId = _eqIndex.get(nodeId).terms().first();
			filter.add(termId);
			mapping[iPos] = termId;
		}
		EntitySet terms = null;
		try {
			terms = _reader.readEntities(filter);
		} catch (java.io.IOException ex) {
			throw new RuntimeException(ex);
		}
		
		this.print(sig.root(), "", "  ", 0, maxDepth, new TermPrinter(terms, mapping, out));
	}
	
	private void print(
			Bucket bucket,
			String indent,
			String extend,
			int depth,
			int maxDepth,
			TermPrinter printer
		) {
		
		if ((depth == maxDepth) || (!bucket.hasChildren())) {
			for (int iNode = bucket.startIndex(); iNode < bucket.endIndex(); iNode++) {
				printer.printTerm(iNode,  indent);
			}
		} else {
			if (!bucket.leftChild().isEmpty()) {
				printer.printLevel(bucket.drop(), true, indent);
				this.print(
						bucket.leftChild(),
						indent + extend,
						extend,
						depth + 1,
						maxDepth,
						printer
					);
			}
			if (!bucket.rightChild().isEmpty()) {
				printer.printLevel(bucket.drop(), false, indent);
				this.print(
						bucket.rightChild(),
						indent + extend,
						extend,
						depth + 1,
						maxDepth,
						printer
				);
			}
		}
	}
}
