
package com.skk.avro.bean;

import org.apache.avro.Schema;

/**
 * Version schema bean
 */
public class VersionedSchema {
    private final int id;
    private final String name;
    private final int version;
    private final Schema schema;

    public VersionedSchema(int id, String name, int version, Schema schema) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public int getVersion() {
        return version;
    }

    public Schema getSchema() {
        return schema;
    }


    public int getId() {
        return id;
    }


}
