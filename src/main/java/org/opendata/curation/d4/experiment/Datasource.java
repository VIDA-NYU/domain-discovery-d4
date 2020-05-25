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
package org.opendata.curation.d4.experiment;

import java.io.BufferedReader;
import java.io.File;

import org.opendata.core.io.FileSystem;

public class Datasource implements Comparable<Datasource> {
	
	private File _directory;
	private File _eqFile;
	private File _termFile;
	private int _size;
	
	public Datasource(File directory, File eqFile, File termFile) throws java.io.IOException {
		
		_directory = directory;
		_eqFile = eqFile;
		_termFile = termFile;
		
		try (BufferedReader in = FileSystem.openReader(_eqFile)) {
			int lineCount = 0;
			while (in.readLine() != null) {
				lineCount++;
			}
			_size = lineCount;
		}
	}
	
	public Datasource(File directory) throws java.io.IOException {
		
		this(
				directory,
				FileSystem.joinPath(directory, "compressed-term-index.TEXT.txt.gz"),
				FileSystem.joinPath(directory, "term-index.txt.gz")
		);
	}

	@Override
	public int compareTo(Datasource source) {
		
		return Integer.compare(_size, source.size());
	}
	
	public File directory() {
		
		return _directory;
	}
	
	public File eqFile() {
		
		return _eqFile;
	}
	
	public String name() {
		
		return _directory.getName();
	}
	
	public int size() {
		
		return _size;
	}
	
	public File termFile() {
		
		return _termFile;
	}
}
