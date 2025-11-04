// ToJsonArrayUDF.java
package org.example;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.io.Text;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

// Using org.codehaus.jackson for compatibility with Hive 3.1.3 classpath
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class ToJSONArrayUDF extends GenericUDF {

    private ListObjectInspector listOI;
    // ObjectMapper is thread-safe for serialization, so we can reuse it.
    // It's initialized lazily or in a static block if preferred, but as a member is common for UDFs.
    private transient ObjectMapper mapper; // transient helps with serialization if the UDF instance itself were serialized

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("ToJsonArrayUDF expects exactly one argument: an array/list of structs.");
        }
        if (!(arguments[0] instanceof ListObjectInspector)) {
            throw new UDFArgumentException("ToJsonArrayUDF expects a LIST type argument, but got " + arguments[0].getTypeName());
        }

        this.listOI = (ListObjectInspector) arguments[0];

        // Ensure the list elements are structs
        if (!(listOI.getListElementObjectInspector() instanceof SettableStructObjectInspector)) {
            throw new UDFArgumentException("ToJsonArrayUDF expects elements of the list to be structs, but got " + listOI.getListElementObjectInspector().getTypeName());
        }

        // Hive will expect a STRING as output
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object listObject = arguments[0].get();
        if (listObject == null) {
            return null;
        }

        // Initialize ObjectMapper if not already done.
        // Doing it here ensures it's initialized on the worker node.
        if (mapper == null) {
            mapper = new ObjectMapper();
            // Optional: Configure for pretty printing or other serialization options
            // mapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
        }

        List<?> list = (List<?>) listOI.getList(listObject);
        if (list == null || list.isEmpty()) {
            return new Text("[]"); // Return empty JSON array for an empty list
        }

        // Convert the Hive List of Structs to a List of Java Maps for Jackson
        List<Map<String, Object>> javaList = new ArrayList<>();
        SettableStructObjectInspector structOI = (SettableStructObjectInspector) listOI.getListElementObjectInspector();
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();

        for (Object structElement : list) {
            if (structElement == null) {
                javaList.add(null); // Preserve nulls in the list
                continue;
            }
            Map<String, Object> javaMap = new HashMap<>();
            for (StructField field : fields) {
                String fieldName = field.getFieldName();
                Object fieldData = structOI.getStructFieldData(structElement, field);
                ObjectInspector fieldOI = field.getFieldObjectInspector();

                // Convert Hive's internal objects to their standard Java equivalents
                Object javaValue = getJavaObject(fieldData, fieldOI);
                javaMap.put(fieldName, javaValue);
            }
            javaList.add(javaMap);
        }

        try {
            StringWriter sw = new StringWriter();
            mapper.writeValue(sw, javaList);
            return new Text(sw.toString());
        } catch (Exception e) {
            // Log the exception for debugging on the cluster
            System.err.println("ToJsonArrayUDF: Error serializing to JSON: " + e.getMessage());
            throw new HiveException("Error serializing to JSON: " + e.getMessage(), e);
        }
    }

    // Helper to extract primitive Java objects from Hive's ObjectInspectors
    private Object getJavaObject(Object hiveObject, ObjectInspector oi) {
        if (hiveObject == null) {
            return null;
        }

        // This handles Primitive Writable objects and returns their Java primitive equivalent
        if (oi instanceof StringObjectInspector) {
            return ((StringObjectInspector) oi).getPrimitiveJavaObject(hiveObject);
        } else if (oi instanceof IntObjectInspector) {
            return ((IntObjectInspector) oi).getPrimitiveJavaObject(hiveObject);
        } else if (oi instanceof DoubleObjectInspector) {
            return ((DoubleObjectInspector) oi).getPrimitiveJavaObject(hiveObject);
        } else if (oi instanceof BooleanObjectInspector) {
            return ((BooleanObjectInspector) oi).getPrimitiveJavaObject(hiveObject);
        }
        // For other types (e.g., more complex nested structs/maps, though not in your current use case),
        // recursive logic or specific handling would be needed.
        // For now, fallback to toString() for unknown primitives, which might not be ideal
        // but generally works for basic types not explicitly handled above.
        return hiveObject.toString();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "ToJsonArrayUDF(" + children[0] + ")";
    }
}