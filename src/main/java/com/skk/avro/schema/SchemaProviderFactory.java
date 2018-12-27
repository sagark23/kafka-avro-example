
package com.skk.avro.schema;

import java.util.Map;

/**
 * Schema provider factory
 * @author sagarkhandelwal23
 */
public interface SchemaProviderFactory {
    SchemaProvider getProvider(Map<String, ?> config) throws Exception;
}
