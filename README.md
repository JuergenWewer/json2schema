You can simply convert a Json to a Avro Schema:

    JsonToSchema jsonToSchema = new JsonToSchema();
    String test = readFileToString(new File("src/test/resources/test1.json"));
    Schema schema = jsonToSchema.convert2AvroSchema(test);
    
Or convert a Json to a Json Schema:

    JsonToSchema jsonToSchema = new JsonToSchema();
    String test = readFileToString(new File("src/test/resources/JsonSchemaTest.json"));
    String schema = jsonToSchema.convert2JsonSchema(test, false);
    
    