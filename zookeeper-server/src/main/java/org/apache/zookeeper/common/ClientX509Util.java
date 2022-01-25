/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientX509Util extends X509Util {

    private static final Logger LOG = LoggerFactory.getLogger(ClientX509Util.class);
    private final String sslAuthProviderProperty = getConfigPrefix() + "authProvider";
    /**
     * The following System Property keys are used to extract clientId from the client cert.
     * @see #getClientId(X509Certificate)
     */
    public final String sslClientCertIdType = getConfigPrefix() + "clientCertIdType";
    public final String sslClientCertIdSanMatchType = getConfigPrefix() + "clientCertIdSanMatchType";
    // Match Regex is used to choose which entry to use
    public final String sslClientCertIdSanMatchRegex = getConfigPrefix() + "clientCertIdSanMatchRegex";
    // Extract Regex is used to construct a client ID (acl entity) to return
    public final String sslClientCertIdSanExtractRegex = getConfigPrefix() + "clientCertIdSanExtractRegex";
    // Specifies match group index for the extract regex (i in Matcher.group(i))
    public final String sslClientCertIdSanExtractMatcherGroupIndex = getConfigPrefix() + "clientCertIdSanExtractMatcherGroupIndex";

    @Override
    protected String getConfigPrefix() {
        return "zookeeper.ssl.";
    }

    @Override
    protected boolean shouldVerifyClientHostname() {
        return false;
    }

    public String getSslAuthProviderProperty() {
        return sslAuthProviderProperty;
    }
    /**
     * Determine the string to be used as the remote host session Id for
     * authorization purposes. Associate this client identifier with a
     * ServerCnxn that has been authenticated over SSL, and any ACLs that refer
     * to the authenticated client.
     *
     * @param clientCert Authenticated X509Certificate associated with the
     *                   remote host.
     * @return Identifier string to be associated with the client.
     */
    public String getClientId(X509Certificate clientCert) {
        String clientCertIdType =
            System.getProperty(sslClientCertIdType);
        if (clientCertIdType != null && clientCertIdType.equalsIgnoreCase("SAN")) {
            try {
                return matchAndExtractSAN(clientCert);
            } catch (Exception ce) {
                LOG.warn("X509AuthenticationProvider::getClientId(): failed to match and extract a"
                    + " client ID from SAN! Using Subject DN instead...", ce);
            }
        }
        // return Subject DN by default
        return clientCert.getSubjectX500Principal().getName();
    }

    private String matchAndExtractSAN(X509Certificate clientCert)
        throws CertificateParsingException {
        Integer matchType =
            Integer.getInteger(sslClientCertIdSanMatchType);
        String matchRegex =
            System.getProperty(sslClientCertIdSanMatchRegex);
        String extractRegex =
            System.getProperty(sslClientCertIdSanExtractRegex);
        Integer extractMatcherGroupIndex = Integer.getInteger(
            sslClientCertIdSanExtractMatcherGroupIndex);
        LOG.info("X509AuthenticationProvider::matchAndExtractSAN(): Using SAN in the client cert "
                + "for client ID! matchType: {}, matchRegex: {}, extractRegex: {}, "
                + "extractMatcherGroupIndex: {}", matchType, matchRegex, extractRegex,
            extractMatcherGroupIndex);
        if (matchType == null || matchRegex == null || extractRegex == null || matchType < 0
            || matchType > 8) {
            // SAN extension must be in the range of [0, 8].
            // See GeneralName object defined in RFC 5280 (The ASN.1 definition of the
            // SubjectAltName extension)
            String errStr = "X509AuthenticationProvider::matchAndExtractSAN(): ClientCert ID type "
                + "SAN was provided but matchType or matchRegex given is invalid! matchType: "
                + matchType + " matchRegex: " + matchRegex;
            LOG.error(errStr);
            throw new IllegalArgumentException(errStr);
        }
        // filter by match type and match regex
        LOG.info("X509AuthenticationProvider::matchAndExtractSAN(): number of SAN entries found in"
            + " clientCert: " + clientCert.getSubjectAlternativeNames().size());
        Pattern matchPattern = Pattern.compile(matchRegex);
        Collection<List<?>> matched = clientCert.getSubjectAlternativeNames().stream().filter(
            list -> list.get(0).equals(matchType) && matchPattern
                .matcher((CharSequence) list.get(1)).find()).collect(Collectors.toList());

        LOG.info(
            "X509AuthenticationProvider::matchAndExtractSAN(): number of SAN entries matched: "
                + matched.size() + ". Printing all matches...");
        for (List<?> match : matched) {
            LOG.info("  Match: (" + match.get(0) + ", " + match.get(1) + ")");
        }

        // if there are more than one match or 0 matches, throw an error
        if (matched.size() != 1) {
            String errStr = "X509AuthenticationProvider::matchAndExtractSAN(): 0 or multiple "
                + "matches found in SAN! Please fix match type and regex so that exactly one match "
                + "is found.";
            LOG.error(errStr);
            throw new IllegalArgumentException(errStr);
        }

        // Extract a substring from the found match using extractRegex
        Pattern extractPattern = Pattern.compile(extractRegex);
        Matcher matcher = extractPattern.matcher(matched.iterator().next().get(1).toString());
        if (matcher.find()) {
            // If extractMatcherGroupIndex is not given, return the 1st index by default
            extractMatcherGroupIndex =
                extractMatcherGroupIndex == null ? 1 : extractMatcherGroupIndex;
            String result = matcher.group(extractMatcherGroupIndex);
            LOG.info("X509AuthenticationProvider::matchAndExtractSAN(): returning extracted "
                + "client ID: {} using Matcher group index: {}", result, extractMatcherGroupIndex);
            return result;
        }
        String errStr = "X509AuthenticationProvider::matchAndExtractSAN(): failed to find an "
            + "extract substring! Please review the extract regex!";
        LOG.error(errStr);
        throw new IllegalArgumentException(errStr);
    }
}
