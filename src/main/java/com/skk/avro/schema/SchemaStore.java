
package com.skk.avro.schema;

import com.skk.avro.bean.VersionedSchema;

public interface SchemaStore extends SchemaProvider {
    public void add(VersionedSchema schema) throws Exception;
}
