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
package org.opendata.core.io;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Generate a list of files. Takes either a directory of files or a text file
 * containing file paths. If any of the referenced files in the text file is a
 * directory the files in the directory will be considered as well.
 * 
 * Matches files in directories to a given file suffix. Note that files that
 * are referenced in a text file are not checked for suffix matches.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class FileListReader {
    
    private final HashSet<String> _suffixes;
    
    public FileListReader(String[] suffixes) {
        
        _suffixes = new HashSet<>();
        for (String suffix : suffixes) {
            _suffixes.add(suffix);
            if (!suffix.endsWith(".gz")) {
                _suffixes.add(suffix + ".gz");
            }
        }
    }
    
    public FileListReader(String suffix) {
        
        this(new String[]{suffix});
    }
    
    /**
     * Include all files in the given directory that match the suffix pattern.
     * 
     * @param dir
     * @param listing 
     */
    private void includeDir(File dir, List<File> listing) {
    
        for (File file : dir.listFiles()) {
            String name = file.getName();
            for (String suffix : _suffixes) {
                if (name.endsWith(suffix)) {
                    listing.add(file);
                    break;
                }
            }
        }
    }
    
    /**
     * Generate a list of files.Expects either a directory or a text file
     * containing references to the files and directories to be included in
     * the listing.
     * 
     * @param inFile
     * @return 
     * @throws java.io.IOException 
     */
    public List<File> listFiles(File inFile) throws java.io.IOException {
        
        List<File> listing = new ArrayList<>();
        
        if (inFile.isDirectory()) {
            this.includeDir(inFile, listing);
        } else {
            try (BufferedReader in = FileSystem.openReader(inFile)) {
                String line;
                while ((line = in.readLine()) != null) {
                    if ((!line.equals("")) && (!line.startsWith("#"))) {
                        File file = new File(line);
                        if (file.isDirectory()) {
                            this.includeDir(file, listing);
                        } else {
                            listing.add(file);
                        }
                    }
                }
            }
        }
        return listing;
    }
}
