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

import org.opendata.core.io.EntitySetReader;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.db.eq.EQIndex;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class SignatureHierarchyToJson {

	private final EQIndex _eqIndex;
	private final EntitySetReader _reader;
	
	public SignatureHierarchyToJson(EQIndex eqIndex, EntitySetReader reader) {
		
		_eqIndex = eqIndex;
		_reader = reader;
	}
	
	public JsonObject toJson(
			HierarchicalSignature sig,
			int maxDepth,
			int maxTerms
	) {
		
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
		
		JsonObject doc = new JsonObject();
		doc.add("name", new JsonPrimitive("Root"));
		doc.add("children", this.toJson(
				sig.root(),
				0,
				maxDepth,
				maxTerms,
				terms,
				mapping
		));
		return doc;
	}
	
	private JsonArray toJson(
			Bucket bucket,
			int depth,
			int maxDepth,
			int maxTerms,
			EntitySet terms,
			int[] nodes
		) {
		
		JsonArray result = new JsonArray();
		
		if ((depth == maxDepth) || (!bucket.hasChildren())) {
			for (int iNode = bucket.startIndex(); iNode < bucket.endIndex(); iNode++) {
				JsonObject node = new JsonObject();
				node.add("name", new JsonPrimitive(terms.get(nodes[iNode]).name()));
				result.add(node);
				if ((result.size() == maxTerms) && (result.size() < bucket.size() - 2)) {
					int remaining = bucket.size() - result.size();
					JsonObject finalNode = new JsonObject();
					finalNode.add("name", new JsonPrimitive(remaining + " more ..."));
					result.add(finalNode);
					break;
				}
			}
		} else {
			if (!bucket.leftChild().isEmpty()) {
				JsonObject child = new JsonObject();
				child.add("name", new JsonPrimitive(
						String.format(
								"Drop (%d) - Left Child",
								bucket.drop()
						)
				));
				child.add("children", this.toJson(
						bucket.leftChild(),
						depth + 1,
						maxDepth,
						maxTerms,
						terms,
						nodes
				));
				result.add(child);
			}
			if (!bucket.rightChild().isEmpty()) {
				JsonObject child = new JsonObject();
				child.add("name", new JsonPrimitive(
						String.format(
								"Drop (%d) - Right Child",
								bucket.drop()
						)
				));
				child.add("children", this.toJson(
						bucket.rightChild(),
						depth + 1,
						maxDepth,
						maxTerms,
						terms,
						nodes
				));
				result.add(child);
			}
		}
		
		return result;
	}
}
