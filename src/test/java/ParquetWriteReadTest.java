import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Value;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.*;
import org.apache.parquet.schema.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class ParquetWriteReadTest {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testMessageTypeBuilder() {
    MessageType expected = Types.buildMessage()
        .required(BINARY).as(UTF8).named("make")
        .optional(BINARY).as(UTF8).named("model")
        .named("car");

    Assert.assertEquals("MessageType should be built correctly", expected, parseMessageType(
            "message car {\n" +
                "required binary make (UTF8);\n" +
                "optional binary model (UTF8);\n" +
            "}"
    ));
  }

  @Test
  public void testWritingParquetFile() throws IOException {
    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();
    Path path = new Path(temp.toString());

    // data
    Car expected = new Car("honda", "civic");

    // writing
    try (ParquetWriter<Car> writer = new Car.WriterBuilder(path).build()) {
      writer.write(expected);
    }

    // reading
    try (ParquetReader<Car> reader = Car.getReader(path)) {
      Car actual = reader.read();
      Assert.assertEquals("Car written should equal car read", expected, actual);
    }
  }

  @Value
  @Builder
  static class Car {
    String make;
    String model;

    static MessageType getSchema() {
      return Types.buildMessage()
          .required(BINARY).as(UTF8).named("make")
          .required(BINARY).as(UTF8).named("model")
          .named("car");
    }

    static class WriterBuilder extends ParquetWriter.Builder<Car, WriterBuilder> {
      WriterBuilder(Path file) {
        super(file);
      }

      protected WriterBuilder self() {
        return this;
      }

      protected WriteSupport<Car> getWriteSupport(Configuration conf) {
        return new WriteSupport<Car>() {
          RecordConsumer recordConsumer;

          @Override
          public WriteContext init(Configuration configuration) {
            return new WriteContext(Car.getSchema(), ImmutableMap.of());
          }

          @Override
          public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
          }

          @Override
          public void write(Car car) {
            recordConsumer.startMessage();
            recordConsumer.startField("make", 0);
            recordConsumer.addBinary(Binary.fromString(car.getMake()));
            recordConsumer.endField("make", 0);
            recordConsumer.startField("model", 1);
            recordConsumer.addBinary(Binary.fromString(car.getModel()));
            recordConsumer.endField("model", 1);
            recordConsumer.endMessage();
          }
        };
      }
    }

    static ParquetReader<Car> getReader(Path path) {
      try {
        return ParquetReader.builder(new ReadSupport<Car>() {

          @Override
          public ReadContext init(InitContext context) {
            return new ReadContext(Car.getSchema());
          }

          @Override
          public RecordMaterializer<Car> prepareForRead(
              Configuration configuration,
              Map<String, String> keyValueMetaData,
              MessageType fileSchema,
              ReadContext readContext) {
            return new RecordMaterializer<Car>() {
              CarGroupConverter converter = new CarGroupConverter();

              @Override
              public Car getCurrentRecord() {
                return converter.getCurrentRecord();
              }

              @Override
              public GroupConverter getRootConverter() {
                return converter;
              }
            };
          }
        }, path).build();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    static class CarGroupConverter extends GroupConverter {
      Car.CarBuilder car = new Car.CarBuilder();

      public Converter getConverter(final int fieldIndex) {
        return new Converter() {
          int idx = fieldIndex;

          @Override
          public boolean isPrimitive() {
            return true;
          }

          @Override
          public PrimitiveConverter asPrimitiveConverter() {
            return new PrimitiveConverter() {
              @Override
              public void addBinary(Binary value) {
                switch (idx) {
                case 0:
                  car.make(value.toStringUsingUTF8());
                  break;
                case 1:
                  car.model(value.toStringUsingUTF8());
                  break;
                }
              }
            };
          }
        };
      }

      public void start() {

      }

      public void end() {

      }

      Car getCurrentRecord() {
        return car.build();
      }
    }
  }
}
