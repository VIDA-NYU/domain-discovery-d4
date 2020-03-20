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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Helper methods for working with files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public final class FileSystem {
    
    /**
     * Copy source file to target.
     * 
     * @param source
     * @param target
     * @throws java.io.IOException 
     */
    public static void copy(File source, File target) throws java.io.IOException {
        
        copy(source.toPath(), target.toPath());
    }

    /**
     * Copy source file to target.
     * 
     * @param source
     * @param target
     * @throws java.io.IOException 
     */
    public static void copy(Path source, Path target) throws java.io.IOException {
        
        Files.copy(
                source,
                target,
                new CopyOption[]{StandardCopyOption.REPLACE_EXISTING}
        );
    }
    
    /**
     * Create a given directory.
     * 
     * Throws an exception if the file exists but is not a directory.
     * 
     * @param file 
     * @return  
     */
    public static File createFolder(File file) {

        if (!file.exists()) {
            file.mkdirs();
        } else if (!file.isDirectory()) {
            throw new java.lang.IllegalArgumentException("Not a directory: " + file.getAbsolutePath());
        }
        
        return file;
    }

    public synchronized static void createParentFolder(File file) {

	if (file.getParentFile() != null) {
	    if (!file.getParentFile().exists()) {
		file.getParentFile().mkdirs();
	    }
	}
    }

    /**
     * Get list of files in given directory sorted in ascending order of their
     * size.
     * 
     * @param directory
     * @return 
     */
    public static List<File> getSortedListOfFiles(File directory) {
	
	ArrayList<File> files = new ArrayList<>();
	files.addAll(Arrays.asList(directory.listFiles()));
	
	/*
	 * Sort files in ascending order of size
	 */
	Collections.sort(files, new Comparator<File>(){
	    @Override
	    public int compare(File f1, File f2) {
		return Long.compare(f1.length(), f2.length());
	    }
	});
	
	return files;
    }
    
    /**
     * Formatting byte size to human readable format.
     * 
     * Source:
     * https://programming.guide/java/formatting-byte-size-to-human-readable-format.html
     * 
     * @param bytes
     * @param si
     * @return 
     */
    public static String humanReadableByteCount(long bytes, boolean si) {
        
        int unit = si ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
    
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static String humanReadableByteCount(long bytes) {
        
        return FileSystem.humanReadableByteCount(bytes, false);
    }
    
    /**
     * Create new file in the given directory with filename.
     * 
     * @param dir
     * @param filename
     * @return 
     */
    public static File joinPath(File dir, String filename) {
	
        if (dir != null) {
            return new File(dir.getAbsolutePath() + File.separator + filename);
        } else {
            return new File(filename);
        }
    }
    
    /**
     * Create new file with relative path to the given directory.
     * 
     * @param dir
     * @param path
     * @return 
     */
    public static File joinPath(File dir, String[] path) {
	
        File file;
        if (dir != null) {
            file = dir;
        } else {
            file = new File(".");
        }
        for (String filename : path) {
            file = new File(file.getAbsolutePath() + File.separator + filename);
        }
        return file;
    }
    
    /**
     * Traverse the directory and all of its sub-directories.Adds all files to
 the given output list.
     * 
     * @param dir
     * @param files 
     * @return  
     */
    public static List<File> listFilesRecursive(File dir, List<File> files) {
        
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                FileSystem.listFilesRecursive(file, files);
            } else {
                files.add(file);
            }
        }
        
        return files;
    }
    
    /**
     * Open a buffered writer for output to a given file.
     * 
     * @param file
     * @return
     * @throws java.io.IOException 
     */
    public static BufferedWriter openBufferedWriter(File file) throws java.io.IOException {
        
        return new BufferedWriter(new OutputStreamWriter(openOutputFile(file)));
    }
    
    
    /**
     * Returns an input stream for the given file.
     * 
     * @param file
     * @return
     * @throws java.io.IOException 
     */
    public static InputStream openFile(File file) throws java.io.IOException {
	
        InputStream is = new FileInputStream(file);
        if (file.getName().endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }

        return is;
    }
    
    public static BufferedReader openReader(File file) throws java.io.IOException {
        
        return new BufferedReader(new InputStreamReader(openFile(file)));
    }
    
    /**
     * Returns an output stream for the given file. Overwrites an existing file.
     * 
     * @param file
     * @return
     * @throws java.io.IOException 
     */
    public static OutputStream openOutputFile(File file) throws java.io.IOException {
	
        return openOutputFile(file, false);
    }
    
    /**
     * Returns an output stream for the given file. The append flag indicates
     * whether output should be appended to an existing file.
     * 
     * @param file
     * @param append
     * @return
     * @throws java.io.IOException 
     */
    public static OutputStream openOutputFile(File file, boolean append) throws java.io.IOException {
	
        OutputStream os = new FileOutputStream(file, append);
        if (file.getName().endsWith(".gz")) {
            os = new GZIPOutputStream(os);
        }

        return os;
    }
    
    public static PrintWriter openPrintWriter(File file, boolean append) throws java.io.IOException {
        
        return new PrintWriter(new OutputStreamWriter(openOutputFile(file, append)));
    }
    
    public static PrintWriter openPrintWriter(File file) throws java.io.IOException {
        
        return openPrintWriter(file, false);
    }
}
