
package com.skk.avro.schema;

import com.skk.avro.bean.VersionedSchema;
import org.apache.avro.Schema;

/**
 * Schema provider interface
 * @author sagarkhandelwal23
 */
public interface SchemaProvider extends AutoCloseable {
    VersionedSchema get(int id);
    VersionedSchema get(String schemaName, int schemaVersion);
    VersionedSchema getMetadata(Schema schema);
}
