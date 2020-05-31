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

import org.opendata.core.set.EntitySet;

public class TermPrinter {

	private final int[] _nodes;
	private final PrintWriter _out;
	private final EntitySet _terms;
	
	public TermPrinter(EntitySet terms, int[] nodes, PrintWriter out) {
		
		_terms = terms;
		_nodes = nodes;
		_out = out;
	}

	public void printLevel(int level, boolean isLeft, String indent) {

		String text;
		if (isLeft) {
			text = String.format("%sDrop (%d) - Left Child", indent, level);
		} else {
			text = String.format("%sDrop (%d) - Right Child", indent, level);
		}
		_out.println(text);
	}
	
	public void printTerm(int index, String indent) {

		_out.println(indent + _terms.get(_nodes[index]).name());
	}
}
