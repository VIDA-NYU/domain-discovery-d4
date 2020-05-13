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
package org.opendata.curation.d4.explore;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.StringHelper;

/**
 * Collect statistics for a set of signature blocks. Maintains a histogram of
 * maximum similarity values, counts the number of signatures, the average,
 * maximum and minimum length (in number of nodes and blocks).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksStatsWriter {

    private static final String COMMAND =
            "Usage:\n" +
            "  <signature-blocks-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SignatureBlocksStatsWriter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File signatureFile = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (
                BufferedReader in = FileSystem.openReader(signatureFile);
                PrintWriter out = FileSystem.openPrintWriter(outputFile);
        ) {
            int count = 0;
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                List<Integer> lengths = new ArrayList<>();
                int sum = 0;
                for (int iToken = 2; iToken < tokens.length; iToken++) {
                   int blockLen = tokens[iToken].split(",").length;
                   sum += blockLen;
                   lengths.add(blockLen);
                }
                String lenlist = StringHelper.joinIntegers(lengths);
                out.println(tokens[0] + "\t" + tokens[1] + "\t" + lengths.size() + "\t" + sum + "\t" + lenlist);
                count++;
                if ((count % 10000) == 0) {
                    System.out.println(count);
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
