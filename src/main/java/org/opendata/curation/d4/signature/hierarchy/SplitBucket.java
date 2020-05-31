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
 * Split bucket in the hierarchy. A split bucket has a left
 * and a right child. It also maintains the drop level at which
 * it was created.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class SplitBucket extends Bucket {

	private Bucket _leftChild;
	private Bucket _rightChild;
	
	public SplitBucket(int startIndex, int pos, int endIndex, int depth, int drop) {
		
		super(startIndex, endIndex, depth, drop);
		
		if (startIndex == endIndex) {
			throw new IllegalArgumentException(
					String.format(
							"Invalid split bucket interval [%d, %d]",
							startIndex,
							endIndex
					)
			);
		}

		_leftChild = new SingleBucket(startIndex, pos, depth + 1, drop);
		_rightChild = new SingleBucket(pos, endIndex, depth + 1, drop);
	}

	@Override
	public Bucket split(int pos, int drop) {
		
		if (_leftChild.contains(pos)) {
			_leftChild = _leftChild.split(pos, drop);
		} else {
			_rightChild = _rightChild.split(pos, drop);
		}
		return this;
	}

	@Override
	public boolean hasChildren() {

		return true;
	}

	@Override
	public Bucket leftChild() {

		return _leftChild;
	}

	@Override
	public int maxDepth() {

		return Math.max(_leftChild.maxDepth(), _rightChild.maxDepth());
	}

	@Override
	public Bucket rightChild() {

		return _rightChild;
	}
}
