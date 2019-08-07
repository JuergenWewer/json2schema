package jw.utils.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class JsonToSchema {

  private static final Map<Class, Schema.Type> classToSchema = new HashMap<>();

  private static StringBuilder sb = new StringBuilder();

  private static final String STRING = "STRING";
  private static final String OBJECT = "OBJECT";
  private static final String ARRAY = "ARRAY";
  private static final String BOOLEAN = "BOOLEAN";
  private static final String INTEGER = "INTEGER";
  private static final String NUMBER = "NUMBER";

  static {
    classToSchema.put(Integer.class, Schema.Type.INT);
    classToSchema.put(Long.class, Schema.Type.LONG);
    classToSchema.put(Float.class, Schema.Type.FLOAT);
    classToSchema.put(Double.class, Schema.Type.DOUBLE);
    classToSchema.put(String.class, Schema.Type.STRING);
    classToSchema.put(ArrayList.class, Schema.Type.ARRAY);
    classToSchema.put(LinkedHashMap.class, Schema.Type.RECORD);
  }

  private final ObjectMapper mapper;
  private Map<String, String> nameMapping;

  public JsonToSchema() {
    mapper = new ObjectMapper();
  }

  public synchronized String convert2JsonSchema(String json, boolean pretty) throws IOException {
    sb = new StringBuilder();
    JsonNode jsonNode = mapper.readTree(json);
    String indent = pretty ? "    " : "";
    String newLine = pretty ? "\n" : "";
    sb.append("{" + newLine + "");
    sb.append("  \"type\": \"object\"," + newLine + "" +
        "  \"properties\": {" + newLine + "");
    sb.append(getJsonSchema(jsonNode, indent, pretty));
    sb.append("  }" + newLine + "");
    sb.append("}");

    return sb.toString();
  }

  private String getJsonSchema(JsonNode jsonNode, String indent, boolean pretty)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    String newLine = pretty ? "\n" : "";
    if (jsonNode.getNodeType().toString().equals(OBJECT)) {
      Iterator<Map.Entry<String, JsonNode>> iter = jsonNode.fields();
      while (iter.hasNext()) {
        Map.Entry<String, JsonNode> jn = iter.next();
        JsonNode node = jn.getValue();
        switch (getType(node)) {
          case STRING:
            sb.append(indent);
            sb.append("\"" + jn.getKey() + "\": { ");
            if (iter.hasNext()) {
              sb.append("\"type\": \"string\" }," + newLine + "");
            } else {
              sb.append("\"type\": \"string\" }" + newLine + "");
            }
            break;
          case BOOLEAN:
            sb.append(indent);
            sb.append("\"" + jn.getKey() + "\": { ");
            if (iter.hasNext()) {
              sb.append("\"type\": \"boolean\" }," + newLine + "");
            } else {
              sb.append("\"type\": \"boolean\" }" + newLine + "");
            }
            break;
          case INTEGER:
            sb.append(indent);
            sb.append("\"" + jn.getKey() + "\": { ");
            if (iter.hasNext()) {
              sb.append("\"type\": \"integer\" }," + newLine + "");
            } else {
              sb.append("\"type\": \"integer\" }" + newLine + "");
            }
            break;
          case NUMBER:
            sb.append(indent);
            sb.append("\"" + jn.getKey() + "\": { ");
            if (iter.hasNext()) {
              sb.append("\"type\": \"number\" }," + newLine + "");
            } else {
              sb.append("\"type\": \"number\" }" + newLine + "");
            }
            break;
          case OBJECT:
            sb.append(indent);
            sb.append("\"" + jn.getKey() + "\": {" + newLine + "" + indent +
                "  \"type\": \"object\"," + newLine + "" + indent +
                "  \"properties\": {" + newLine + "");
            sb.append(getJsonSchema(node, pretty ? indent + "    " : "", pretty));
            sb.append(indent + "  }" + newLine + "" +
                indent + "}" + newLine + "");
            break;
          case ARRAY:
            sb.append(indent);
            sb.append("\"" + jn.getKey() + "\": {" + newLine + "" + indent +
                "  \"type\": \"array\"," + newLine + "" + indent +
                "  \"items\": {" + newLine + "");
            sb.append(getJsonSchema(node, pretty ? indent + "    " : "", pretty));
            sb.append(indent + "  }" + newLine + "" +
                indent + "}" + newLine + "");
            break;
          default:
            throw new IOException("Unknown schema type: " + getType(node));
        }
      }
    } else {
      Iterator<JsonNode> iter = jsonNode.iterator();
      while (iter.hasNext()) {
        JsonNode jn = iter.next();
        switch (getType(jn)) {
          case STRING:
            sb.append(indent);
            sb.append("\"type\": \"string\"" + newLine + "");
            break;
          case BOOLEAN:
            sb.append(indent);
            sb.append("\"type\": \"boolean\"" + newLine + "");
            break;
          case INTEGER:
            sb.append(indent);
            sb.append("\"type\": \"integer\"" + newLine + "");
            break;
          case NUMBER:
            sb.append(indent);
            sb.append("\"type\": \"number\"" + newLine + "");
            break;
          case OBJECT:
            sb.append(indent);
            sb.append("\"type\": \"object\"," + newLine + "" + indent +
                "  \"properties\": {" + newLine + "");
            sb.append(getJsonSchema(jn, pretty ? indent + "    " : "", pretty));
            sb.append(indent + "}" + newLine + "");
            break;
          case ARRAY:
            sb.append(indent);
            sb.append("\"" + "items" + "\": {" + newLine + "" + indent +
                "  \"type\": \"array\"," + newLine + "" + indent +
                "  \"items\": {" + newLine + "");
            sb.append(getJsonSchema(jn, pretty ? indent + "    " : "", pretty));
            sb.append(indent + "  }" + newLine + "" +
                indent + "}" + newLine + "");
            break;
          default:
            throw new IOException("Unknown schema type: " + getType(jn));
        }
        break;
      }
    }
    return sb.toString();
  }

  private static String getType(JsonNode node) throws IOException {
    val value = node.getNodeType();
    if (value == null) {
      return null;
    } else if (value.toString().equals("Byte")) {
      return "Schema.Type.INT8";
    } else if (value.toString().equals("Short")) {
      return "Schema.Type.INT16";
    } else if (value.toString().equals("NUMBER")) {
      if (node.toString().contains(".") || node.toString().contains(",")) {
        return NUMBER;
      } else {
        return INTEGER;
      }
    } else if (value.toString().equals("Long")) {
      return "Schema.Type.INT64";
    } else if (value.toString().equals("Float")) {
      return "Schema.Type.FLOAT32";
    } else if (value.toString().equals("Double")) {
      return "Schema.Type.FLOAT64";
    } else if (value.toString().equals("BOOLEAN")) {
      return BOOLEAN;
    } else if (value.toString().equals("STRING") || value.toString().equals("NULL")) {
      return STRING;
    } else if (value.toString().equals("ARRAY")) {
      return ARRAY;
    } else if (value.toString().equals("OBJECT")) {
      return OBJECT;
    } else if (value.toString().equals("Map")) {
      return "Schema.Type.MAP";
    } else {
      throw new IOException("Unknown Java type for schemaless data: " + value.getClass());
    }
  }


  public synchronized Schema convert2AvroSchema(String json, String name, String namespace) throws IOException {
    Map<String, Object> metrics = mapper.readValue(json, LinkedHashMap.class);

    nameMapping = new HashMap<>();
    Schema schema = parse2Schema(metrics, name, namespace);

    return schema;
  }

  public synchronized GenericRecord convert2GenericRecord(String json, String name, String namespace) throws IOException {
    Map<String, Object> metrics = mapper.readValue(json, LinkedHashMap.class);

    nameMapping = new HashMap<>();
    Schema schema = parse2Schema(metrics, name, namespace);
    GenericRecord record = new GenericData.Record(schema);
    fillRecord(record, schema, metrics);

    return record;
  }

  private void fillRecord(GenericRecord record, Schema schema, Map<String, Object> metrics) {
    for (Schema.Field field : schema.getFields()) {
      String name = nameMapping.get(field.name());
      if (name != null) {
        Schema fieldSchema = field.schema().getTypes().get(1);
        if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
          GenericRecord fieldRecord = new GenericData.Record(fieldSchema);
          fillRecord(fieldRecord, fieldSchema, (Map<String, Object>) metrics.get(name));
          record.put(field.name(), fieldRecord);
        } else {
          record.put(field.name(), metrics.get(name));
        }
      }
    }
  }


  private Schema parse2Schema(Map<String, Object> metrics, String name, String namespace) {
    Schema schema = Schema.createRecord(name, null, namespace, false);
    List<Schema.Field> fields = new ArrayList<>();

    for (Map.Entry<String, Object> entry : metrics.entrySet()) {
      String lookupName = getLookupName(entry.getKey());
      nameMapping.put(lookupName, entry.getKey());

      Schema.Field field = new Schema.Field(lookupName,
          Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
              parseRecordField(entry.getKey(), entry.getValue(), namespace))),
          null,
          Schema.parseJson("null"));

      fields.add(field);
    }

//        fields.add(new Schema.Field("logLine", LogLine.getClassSchema(), null, null));
    schema.setFields(fields);

    return schema;
  }

  private String getLookupName(String name) {
    String lookupName = name;
    if (name.matches("[0-9]+")) {
      lookupName = "metric" + name;
    }

    lookupName = lookupName.replace(".", "").replace("%", "").replace("-", "");
    return lookupName;
  }

  public Schema parseRecordField(String name, Object value, String namespace) {
    String lookupRecordName = getLookupName(name);
    nameMapping.put(lookupRecordName, name);

    if (value instanceof Integer) {
      return Schema.create(Schema.Type.INT);
    } else if (value instanceof Long) {
      return Schema.create(Schema.Type.LONG);
    } else if (value instanceof Float) {
      return Schema.create(Schema.Type.FLOAT);
    } else if (value instanceof Double) {
      return Schema.create(Schema.Type.DOUBLE);
    } else if (value instanceof String) {
      return Schema.create(Schema.Type.STRING);
    } else if (value instanceof Boolean) {
      return Schema.create(Schema.Type.BOOLEAN);
    } else if (value instanceof LinkedHashMap) {
      Schema record = Schema.createRecord(lookupRecordName, null, namespace, false);
      List<Schema.Field> fields = new ArrayList<>();
      Map<String, Object> jsonField = (Map<String, Object>) value;

      for (Map.Entry<String, Object> e : jsonField.entrySet()) {
        String lookupName = getLookupName(e.getKey());
        nameMapping.put(lookupName, e.getKey());

        Schema valueSchema = parseRecordField(e.getKey(), e.getValue(), namespace);
        if (valueSchema == null) {
          Schema.Field field = new Schema.Field(lookupName,
              Schema.createUnion(
                  Arrays.asList(Schema.create(Schema.Type.NULL), parseRecordField(e.getKey(), "", namespace))),
              null,
              Schema.parseJson("null"));
          fields.add(field);
        } else {
          Schema.Field field = new Schema.Field(lookupName,
              Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), valueSchema)),
              null,
              Schema.parseJson("null"));
          fields.add(field);
        }
      }

      record.setFields(fields);
      return record;
    } else if (value instanceof ArrayList) {
      ArrayList list = (ArrayList) value;
      Schema elementSchema = Schema.create(Schema.Type.NULL);
      if (!list.isEmpty()) {
        elementSchema = parseRecordField(name, list.get(0), namespace);
      }

      return Schema.createArray(elementSchema);
    }

    return null;
  }
}
