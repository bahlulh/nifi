/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.azure.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo.Builder;
import org.apache.nifi.processor.util.list.ListedEntityTracker;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({DeleteAzureDataLakeStorage.class})
@CapabilityDescription("Puts content into an Azure Data Lake Storage Gen 2")
@WritesAttributes({@WritesAttribute(attribute = "azure.filesystem", description = "The name of the Azure File System"),
        @WritesAttribute(attribute = "azure.directory", description = "The name of the Azure Directory"),
        @WritesAttribute(attribute = "azure.filename", description = "The name of the Azure File Name"),
        @WritesAttribute(attribute = "azure.timestamp", description = "The timestamp in Azure for the blob"),
        @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for file content"),
        @WritesAttribute(attribute = "azure.length", description = "Length of the file")})
@InputRequirement(Requirement.INPUT_REQUIRED)

public class ListAzureDataLakeStorage extends AbstractListProcessor<BlobInfo> {

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(
                Arrays.asList(
                    LISTING_STRATEGY,
                    AbstractAzureDataLakeStorageProcessor.ACCOUNT_NAME,
                    AbstractAzureDataLakeStorageProcessor.ACCOUNT_KEY,
                    AbstractAzureDataLakeStorageProcessor.FILESYSTEM,
                    AbstractAzureDataLakeStorageProcessor.DIRECTORY));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        results.addAll(AzureStorageUtils.validateCredentialProperties(validationContext));
        AzureStorageUtils.validateProxySpec(validationContext, results);
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected String getDefaultTimePrecision() {
        return PRECISION_SECONDS.getValue();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return true;
    }

    @Override
    protected Map<String, String> createAttributes(BlobInfo entity, ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return "";
    }

    @Override
    protected List<BlobInfo> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
        final String fileSystem = context.getProperty(AbstractAzureDataLakeStorageProcessor.FILESYSTEM).getValue();
        final String directory = context.getProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY).getValue();

        final List<BlobInfo> listing = new ArrayList<>();
        try {
            final DataLakeServiceClient storageClient = AbstractAzureDataLakeStorageProcessor.getStorageClient(context, null);
            final DataLakeFileSystemClient dataLakeFileSystemClient = storageClient.getFileSystemClient(fileSystem);
            ListPathsOptions options = new ListPathsOptions();
            options.setPath(directory);

            java.util.Iterator<PathItem> iterator = dataLakeFileSystemClient.listPaths(options, null).iterator();
            PathItem item = iterator.next();
            while (item != null){
                Builder builder = new BlobInfo.Builder().blobName(item.getName());
                if (!iterator.hasNext()){
                    break;
                }

                listing.add(builder.build());
                item = iterator.next();
            }
        } catch (Throwable t) {
            getLogger().info("Original Exception: " + ExceptionUtils.getStackTrace(t));
            throw new IOException(ExceptionUtils.getRootCause(t));
        }
        return listing;
    }
}