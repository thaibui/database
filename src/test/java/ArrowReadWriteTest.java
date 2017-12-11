import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bui.database.schema.ArrowSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Read/Write data to a file using ArrowSchema API
 */
public class ArrowReadWriteTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
    allocator = null;
  }

  @Test
  public void testReadWriteSchema() throws IOException {
    Schema expected = ArrowSchema.builder()
        .utf8Field("id")
        .structField("person", ArrowSchema.builder()
            .int32UnsignedField("id")
            .utf8Field("name"))
        .listField("addresses", ArrowSchema.structBuilder(ArrowSchema.builder()
            .utf8Field("city")
            .utf8Field("zipcode")))
        .binaryField("binary")
        .booleanField("boolean")
        .floatingPointSinglePrecisionField("double")
        .int32SignedField("int32_signed")
        .nullField("null")
        .build();

    Assert.assertEquals("JSON serde should work", expected, Schema.fromJSON(expected.toJson()));

    VectorSchemaRoot rootSchema = VectorSchemaRoot.create(expected, allocator);

    Assert.assertEquals("Schema should be correctly constructed",
        "Schema<"
            + "id: Utf8, "
            + "person: Struct<id: Int(32, false), name: Utf8>, "
            + "addresses: List<Struct<city: Utf8, zipcode: Utf8>>, "
            + "binary: Binary, "
            + "boolean: Bool, "
            + "double: FloatingPoint(SINGLE), "
            + "int32_signed: Int(32, true), "
            + "null: Null"
            + ">",
        rootSchema.getSchema().toString());
  }
}
