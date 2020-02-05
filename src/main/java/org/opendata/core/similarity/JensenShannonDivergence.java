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
package org.opendata.core.similarity;

/**
 * From http://www.java2s.com/Code/Java/Security/ReturnstheJensenShannondivergence.htm
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JensenShannonDivergence {
    
    /**
     * Returns the Jensen-Shannon divergence.
     * @param p1
     * @param p2
     * @return 
     */
    public static double jsd(double[] p1, double[] p2) {

        double[] average = new double[p1.length];
        for (int i = 0; i < p1.length; ++i) {
            average[i] += (p1[i] + p2[i])/2;
        }
        return (kld(p1, average) + kld(p2, average))/2;
    }

   /**
     * Returns the KullbackLeiblerDivergence divergence, K(p1 || p2).
     *
     * The log is w.r.t. base 2. <p>
     *
     * *Note*: If any value in <tt>p2</tt> is <tt>0.0</tt> then the KL-divergence
     * is <tt>infinite</tt>. Limin changes it to zero instead of infinite. 
     * 
     * @param p1
     * @param p2
     * @return 
     */
    public static double kld(double[] p1, double[] p2) {

        double kl = 0.0;

        for (int i = 0; i < p1.length; ++i) {
            if ((p1[i] != 0) && (p2[i] != 0.0)) {
                kl += p1[i] * Math.log(p1[i] / p2[i]);
            }
        }

        return kl;
    }
}
