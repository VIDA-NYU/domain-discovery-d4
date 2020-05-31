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
 * Single interval bucket in the hierarchy.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class SingleBucket extends Bucket {

	public SingleBucket(int startIndex, int endIndex, int depth, int drop) {
		
		super(startIndex, endIndex, depth, drop);
	}
	
	/**
	 * The root node is at depth zero and created by no drop.
	 * 
	 * @param startIndex
	 * @param endIndex
	 */
	public SingleBucket(int startIndex, int endIndex) {
		
		this(startIndex, endIndex, 0, 0);
	}

	@Override
	public Bucket split(int pos, int level) {

		return new SplitBucket(
				this.startIndex(),
				pos,
				this.endIndex(),
				this.depth(),
				level
		);
	}

	@Override
	public boolean hasChildren() {

		return false;
	}

	@Override
	public Bucket leftChild() {

		return null;
	}

	@Override
	public int maxDepth() {

		return this.depth();
	}

	@Override
	public Bucket rightChild() {

		return null;
	}
}
