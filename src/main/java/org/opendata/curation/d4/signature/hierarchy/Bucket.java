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

/**
 * Bucket in a cluster hierarchy. There are two types of buckets:
 * single bucket and split bucket.
 *  
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public abstract class Bucket {

	private final int _depth;
	private final int _drop;
	private final int _endIndex;
	private final int _startIndex;
	
	public Bucket(int startIndex, int endIndex, int depth, int drop) {
		
		if (startIndex > endIndex) {
			throw new IllegalArgumentException(
					String.format(
							"Invalid bucket interval [%d, %d]",
							startIndex,
							endIndex
					)
			);
		}
		
		_startIndex = startIndex;
		_endIndex = endIndex;
		_depth = depth;
		_drop = drop;
	}

	/**
	 * Returns true if the given index positions falls within the
	 * range of this bucket.
	 * 
	 * @param pos
	 * @return
	 */
	public boolean contains(int pos) {
		
		return (_startIndex <= pos) && (pos < _endIndex);
	}
	
	/**
	 * Depth of the node in the hierarchy. The root node is at
	 * depth zero.
	 * 
	 * @return
	 */
	public int depth() {
		
		return _depth;
	}
	
	/**
	 * Number of the drop in the context signature that created the bucket.
	 * 
	 * @return
	 */
	public int drop() {
		
		return _drop;
	}
	
	/**
	 * Right boundary of the interval (exclusive).
	 * 
	 * @return
	 */
	public int endIndex() {
		
		return _endIndex;
	}

	/**
	 * A split bucket has children while the single bucket does not.
	 * 
	 * @return
	 */
	public abstract boolean hasChildren();
	
	/**
	 * Test if the set of nodes in the bucket is empty.
	 * 
	 * @return
	 */
	public boolean isEmpty() {
		
		return (_startIndex == _endIndex);
	}
	
	/**
	 * Get the left child bucket for a split node. For a single node the
	 * result is null.
	 * 
	 * @return
	 */
	public abstract Bucket leftChild();
	
	/**
	 * Returns the maximum depth subtree of the bucket.
	 * 
	 * @return
	 */
	public abstract int maxDepth();
	
	/**
	 * Get the right child bucket for a split node. For a single node the
	 * result is null.
	 * 
	 * @return
	 */
	public abstract Bucket rightChild();
	
	/**
	 * Add a split to the hierarchy represented by the bucket at the
	 * given index position.
	 * 
	 * @param pos
	 * @param drop
	 * @return
	 */
	public abstract Bucket split(int pos, int drop);
	
	/**
	 * Total number of nodes in the bucket.
	 * 
	 * @return
	 */
	public int size() {
		
		return _endIndex - _startIndex;
	}
	
	/**
	 * Left boundary of the interval (inclusive).
	 * 
	 * @return
	 */
	public int startIndex() {
		
		return _startIndex;
	}
}
