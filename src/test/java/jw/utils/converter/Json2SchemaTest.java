package jw.utils.converter;

import static org.apache.commons.io.FileUtils.readFileToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

@Slf4j
public class Json2SchemaTest {

  JsonToSchema cut = new JsonToSchema();

  @Test
  public void schematest() throws IOException {
    String test = readFileToString(new File("src/test/resources/test1.json"));
    Schema schema = cut.convert2AvroSchema(test, "Metrics", "juergenwewer");
    assertEquals(readFileToString(new File("src/test/resources/test1.avsc")), schema.toString());
  }

  @Test
  public void schemaWithNulltest() throws IOException {
    String test = readFileToString(new File("src/test/resources/test2.json"));
    Schema schema = cut.convert2AvroSchema(test, "Metrics", "juergenwewer");
    assertEquals(readFileToString(new File("src/test/resources/test2.avsc")), schema.toString());
  }

  @Test
  public void jsonschematest() throws IOException {
    String test = readFileToString(new File("src/test/resources/test3.json"));
    String schema = cut.convert2JsonSchema(test, true);
    assertEquals(readFileToString(new File("src/test/resources/test3.schema.json")), schema);
  }

  @Test
  public void jsonschemaArraytest() throws IOException {
    String test = readFileToString(new File("src/test/resources/test4.json"));
    String schema = cut.convert2JsonSchema(test, false);
    assertEquals(readFileToString(new File("src/test/resources/test4.schema.json")), schema);
  }

  @Test
  public void JsonSegementschematest() throws IOException {
    String test = readFileToString(new File("src/test/resources/JsonSchemaTest.json"));
    String schema = cut.convert2JsonSchema(test, false);
    assertEquals(readFileToString(new File("src/test/resources/JsonSchemaTest.avsc")), schema);
  }

  @Test
  public void AvroSegementschematest() throws IOException {
    String test = readFileToString(new File("src/test/resources/AvroSchemaTest.json"));
    Schema schema = cut.convert2AvroSchema(test, "Metrics", "juergenwewer");
    assertEquals(readFileToString(new File("src/test/resources/AvroSchematest.avsc")),
        schema.toString());
  }


  @Test
  public void generalRecordtest() throws IOException {
    String test = readFileToString(new File("src/test/resources/test1.json"));
    GenericRecord gr = cut.convert2GenericRecord(test, "Metrics", "juergenwewer");
    assertNotNull(gr);
  }

  @Test
  public void generalRecordSegmenttest() throws IOException {
    String test = readFileToString(new File("src/test/resources/AvroSchemaTest.json"));
    GenericRecord gr = cut.convert2GenericRecord(test, "Metrics", "juergenwewer");
    assertNotNull(gr);
  }

}
