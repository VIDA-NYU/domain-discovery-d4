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
package org.opendata.core.prune;

import java.util.ArrayList;
import java.util.List;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.ImmutableIDSet;
import org.opendata.core.util.StringHelper;

/**
 * For a given list of identifiable double, find the pruning index for an
 * implementation-specific pruning condition.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public abstract class CandidateSetFinder <T extends IdentifiableDouble> {
   
    // Drop finder names
    public static final String MAX_DIFF = "MAX-DIFF";
    public static final String MAX_DIFF_THRESHOLD = "MAX-DIFF-THRESHOLD";
    public static final String THRESHOLD = "THRESHOLD";
    
    // Drop finder specification syntax
    public static final String MAXDIFFFINDER =
            MAX_DIFF +
                ":<threshold-constraint>" +
                ":<full-set-constraint>[true | false]" +
                ":<ignore-last-drop>[true | false]";
    public static final String MAXDIFFTHRESHOLDFINDER =
            MAX_DIFF_THRESHOLD +
                ":<threshold-constraint>" +
                ":<full-set-constraint>[true | false]" +
                ":<ignore-last-drop>[true | false]";
    public final static String THRESHOLDFINDER =
            THRESHOLD + ":<threshold-constraint>";
    
    /**
     * Print command line statement for drop finder arguments.
     * 
     * @param indent
     * @return 
     */
    public static String getCommand(String indent) {
     
        return indent + MAXDIFFFINDER + " |\n" +
                indent + MAXDIFFTHRESHOLDFINDER + " |\n" +
                indent + THRESHOLDFINDER;
    }

    /**
     * Get candidate set finder instance from specification string.
     * 
     * @param spec
     * @return 
     */
    public static CandidateSetFinder getFunction(String spec) {
	
        String[] tokens = spec.split(":");
        
        try {
            String name = tokens[0];
            if (name.equalsIgnoreCase(MAX_DIFF)) {
                if (tokens.length == 4) {
                    return new MaxDropFinder(
                            Threshold.getConstraint(tokens[1]),
                            Boolean.parseBoolean(tokens[2]),
                            Boolean.parseBoolean(tokens[3])
                    );
                }
            } else if (name.equalsIgnoreCase(MAX_DIFF_THRESHOLD)) {
                if (tokens.length == 4) {
                    return new MaxDropThresholdFinder(
                            Threshold.getConstraint(tokens[1]),
                            Boolean.parseBoolean(tokens[2]),
                            Boolean.parseBoolean(tokens[3])
                    );
                }
            } else if (name.equalsIgnoreCase(THRESHOLD)) {
                if (tokens.length == 2) {
                    return new ThresholdFinder(
                            Threshold.getConstraint(tokens[1])
                    );
                }
            } else {
                throw new java.lang.IllegalArgumentException("Unknown candidate set finder: " + name);
            }
        } catch (java.lang.NumberFormatException ex) {
        }
        throw new java.lang.IllegalArgumentException("Invalid candidate set finder specification: " + spec);
    }

    /**
     * Return the pruning index.
     * 
     * @param elements
     * @return 
     */
    public int getPruneIndex(List<T> elements) {
        
        return this.getPruneIndex(elements, 0);
    }
    
    /**
     * Return pruning index after the given start position.
     * 
     * @param elements
     * @param start
     * @return 
     */
    public abstract int getPruneIndex(List<T> elements, int start);
    
    /**
     * Return identifier of elements that occur before the pruning index.
     * 
     * @param elements
     * @return 
     */
    public IDSet pruneElements(List<T> elements) {
        
        int pruneIndex = this.getPruneIndex(elements);
        if (pruneIndex > 0) {
            List<Integer> result = new ArrayList<>();
            for (int iIndex = 0; iIndex < pruneIndex; iIndex++) {
                result.add(elements.get(iIndex).id());
            }
            return new ImmutableIDSet(result);
        } else {
            return new ImmutableIDSet();
        }
    }    
    
    /**
     * Validate a given drop finder specification.
     * 
     * Return the given specification if valid. Will raise
     * IllegalArgumentException if specification is not valid.
     * 
     * @param spec
     * @return 
     */
    public static String validateSpecification(String spec) {
        
        String[] tokens = spec.split(":");
        
        String message = "Invalid candidate set finder specification: " + spec;
        
        String name = tokens[0];
        if (name.equalsIgnoreCase(MAX_DIFF)) {
            if (tokens.length == 4) {
                Threshold.validateSpecification(tokens[1]);
            } else {
                throw new java.lang.IllegalArgumentException(message);
            }
        } else if (name.equalsIgnoreCase(MAX_DIFF_THRESHOLD)) {
            if (tokens.length == 4) {
                Threshold.validateSpecification(tokens[1]);
            } else {
                throw new java.lang.IllegalArgumentException(message);
            }
        } else if (name.equalsIgnoreCase(THRESHOLD)) {
            Threshold
                    .validateSpecification(StringHelper.joinStrings(tokens, 1, ":"));
        } else {
            throw new java.lang.IllegalArgumentException("Unknown drop finder name: " + name);
        }
        
        return spec;
    }
}
