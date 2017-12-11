package org.bui.database.schema;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An Arrow {@link org.apache.arrow.vector.types.pojo.Schema} builder.
 *
 * <pre>
 * Schema expected = ArrowSchema.builder()
 *   .utf8Field("id")
 *   .structField("person", ArrowSchema.builder()
 *     .int32UnsignedField("id")
 *     .utf8Field("name"))
 *   .listField("addresses", ArrowSchema.structBuilder(ArrowSchema.builder()
 *     .utf8Field("city")
 *     .utf8Field("zipcode")))
 *   .binaryField("binary")
 *   .booleanField("boolean")
 *   .floatingPointSinglePrecisionField("double")
 *   .int32SignedField("int32_signed")
 *   .nullField("null")
 *   .build();
 * </pre>
 */
public class ArrowSchema {

  private final ImmutableList.Builder<Field> fieldBuilder;

  private ArrowSchema() {
    this.fieldBuilder = new ImmutableList.Builder<>();
  }

  public static ArrowSchema builder() {
    return new ArrowSchema();
  }

  public static ArrowSchema structBuilder(ArrowSchema builder) {
    return new ArrowSchema().structField(null, builder);
  }

  public ArrowSchema structField(String name, ArrowSchema builder) {
    fieldBuilder.add(new Field(name, FieldType.nullable(new ArrowType.Struct()), builder.fieldBuilder.build()));

    return this;
  }

  public ArrowSchema utf8Field(String name) {
    fieldBuilder.add(Field.nullable(name, new ArrowType.Utf8()));

    return this;
  }

  public ArrowSchema int32UnsignedField(String name) {
    fieldBuilder.add(Field.nullablePrimitive(name, new ArrowType.Int(32, false)));

    return this;
  }

  public ArrowSchema int32SignedField(String name) {
    fieldBuilder.add(Field.nullablePrimitive(name, new ArrowType.Int(32, true)));

    return this;
  }

  public ArrowSchema nullField(String name) {
    fieldBuilder.add(Field.nullable(name, new ArrowType.Null()));

    return this;
  }

  public ArrowSchema floatingPointSinglePrecisionField(String name) {
    fieldBuilder.add(Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));

    return this;
  }

  public ArrowSchema binaryField(String name) {
    fieldBuilder.add(Field.nullable(name, new ArrowType.Binary()));

    return this;
  }

  public ArrowSchema booleanField(String name) {
    fieldBuilder.add(Field.nullable(name, new ArrowType.Bool()));

    return this;
  }

  public ArrowSchema listField(String name, ArrowSchema builder) {
    fieldBuilder.add(new Field(name, FieldType.nullable(new ArrowType.List()), builder.fieldBuilder.build()));

    return this;
  }

  public Schema build() {
    return new Schema(fieldBuilder.build());
  }
}
