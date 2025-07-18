/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.ross.serializer;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class OrderAvro extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 5604831584917532121L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderAvro\",\"namespace\":\"com.ross.serializer\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"shipping\",\"type\":\"double\"},{\"name\":\"imageData\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ItemAvro\",\"fields\":[{\"name\":\"sku\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"double\"}]}}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<OrderAvro> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<OrderAvro> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<OrderAvro> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<OrderAvro> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<OrderAvro> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this OrderAvro to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a OrderAvro from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a OrderAvro instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static OrderAvro fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.lang.CharSequence orderId;
  private double shipping;
  private java.nio.ByteBuffer imageData;
  private java.util.List<com.ross.serializer.ItemAvro> items;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public OrderAvro() {}

  /**
   * All-args constructor.
   * @param orderId The new value for orderId
   * @param shipping The new value for shipping
   * @param imageData The new value for imageData
   * @param items The new value for items
   */
  public OrderAvro(java.lang.CharSequence orderId, java.lang.Double shipping, java.nio.ByteBuffer imageData, java.util.List<com.ross.serializer.ItemAvro> items) {
    this.orderId = orderId;
    this.shipping = shipping;
    this.imageData = imageData;
    this.items = items;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return orderId;
    case 1: return shipping;
    case 2: return imageData;
    case 3: return items;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: orderId = (java.lang.CharSequence)value$; break;
    case 1: shipping = (java.lang.Double)value$; break;
    case 2: imageData = (java.nio.ByteBuffer)value$; break;
    case 3: items = (java.util.List<com.ross.serializer.ItemAvro>)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'orderId' field.
   * @return The value of the 'orderId' field.
   */
  public java.lang.CharSequence getOrderId() {
    return orderId;
  }


  /**
   * Sets the value of the 'orderId' field.
   * @param value the value to set.
   */
  public void setOrderId(java.lang.CharSequence value) {
    this.orderId = value;
  }

  /**
   * Gets the value of the 'shipping' field.
   * @return The value of the 'shipping' field.
   */
  public double getShipping() {
    return shipping;
  }


  /**
   * Sets the value of the 'shipping' field.
   * @param value the value to set.
   */
  public void setShipping(double value) {
    this.shipping = value;
  }

  /**
   * Gets the value of the 'imageData' field.
   * @return The value of the 'imageData' field.
   */
  public java.nio.ByteBuffer getImageData() {
    return imageData;
  }


  /**
   * Sets the value of the 'imageData' field.
   * @param value the value to set.
   */
  public void setImageData(java.nio.ByteBuffer value) {
    this.imageData = value;
  }

  /**
   * Gets the value of the 'items' field.
   * @return The value of the 'items' field.
   */
  public java.util.List<com.ross.serializer.ItemAvro> getItems() {
    return items;
  }


  /**
   * Sets the value of the 'items' field.
   * @param value the value to set.
   */
  public void setItems(java.util.List<com.ross.serializer.ItemAvro> value) {
    this.items = value;
  }

  /**
   * Creates a new OrderAvro RecordBuilder.
   * @return A new OrderAvro RecordBuilder
   */
  public static com.ross.serializer.OrderAvro.Builder newBuilder() {
    return new com.ross.serializer.OrderAvro.Builder();
  }

  /**
   * Creates a new OrderAvro RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new OrderAvro RecordBuilder
   */
  public static com.ross.serializer.OrderAvro.Builder newBuilder(com.ross.serializer.OrderAvro.Builder other) {
    if (other == null) {
      return new com.ross.serializer.OrderAvro.Builder();
    } else {
      return new com.ross.serializer.OrderAvro.Builder(other);
    }
  }

  /**
   * Creates a new OrderAvro RecordBuilder by copying an existing OrderAvro instance.
   * @param other The existing instance to copy.
   * @return A new OrderAvro RecordBuilder
   */
  public static com.ross.serializer.OrderAvro.Builder newBuilder(com.ross.serializer.OrderAvro other) {
    if (other == null) {
      return new com.ross.serializer.OrderAvro.Builder();
    } else {
      return new com.ross.serializer.OrderAvro.Builder(other);
    }
  }

  /**
   * RecordBuilder for OrderAvro instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<OrderAvro>
    implements org.apache.avro.data.RecordBuilder<OrderAvro> {

    private java.lang.CharSequence orderId;
    private double shipping;
    private java.nio.ByteBuffer imageData;
    private java.util.List<com.ross.serializer.ItemAvro> items;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.ross.serializer.OrderAvro.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.orderId)) {
        this.orderId = data().deepCopy(fields()[0].schema(), other.orderId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.shipping)) {
        this.shipping = data().deepCopy(fields()[1].schema(), other.shipping);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.imageData)) {
        this.imageData = data().deepCopy(fields()[2].schema(), other.imageData);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.items)) {
        this.items = data().deepCopy(fields()[3].schema(), other.items);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing OrderAvro instance
     * @param other The existing instance to copy.
     */
    private Builder(com.ross.serializer.OrderAvro other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.orderId)) {
        this.orderId = data().deepCopy(fields()[0].schema(), other.orderId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.shipping)) {
        this.shipping = data().deepCopy(fields()[1].schema(), other.shipping);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.imageData)) {
        this.imageData = data().deepCopy(fields()[2].schema(), other.imageData);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.items)) {
        this.items = data().deepCopy(fields()[3].schema(), other.items);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'orderId' field.
      * @return The value.
      */
    public java.lang.CharSequence getOrderId() {
      return orderId;
    }


    /**
      * Sets the value of the 'orderId' field.
      * @param value The value of 'orderId'.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder setOrderId(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.orderId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'orderId' field has been set.
      * @return True if the 'orderId' field has been set, false otherwise.
      */
    public boolean hasOrderId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'orderId' field.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder clearOrderId() {
      orderId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'shipping' field.
      * @return The value.
      */
    public double getShipping() {
      return shipping;
    }


    /**
      * Sets the value of the 'shipping' field.
      * @param value The value of 'shipping'.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder setShipping(double value) {
      validate(fields()[1], value);
      this.shipping = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'shipping' field has been set.
      * @return True if the 'shipping' field has been set, false otherwise.
      */
    public boolean hasShipping() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'shipping' field.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder clearShipping() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'imageData' field.
      * @return The value.
      */
    public java.nio.ByteBuffer getImageData() {
      return imageData;
    }


    /**
      * Sets the value of the 'imageData' field.
      * @param value The value of 'imageData'.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder setImageData(java.nio.ByteBuffer value) {
      validate(fields()[2], value);
      this.imageData = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'imageData' field has been set.
      * @return True if the 'imageData' field has been set, false otherwise.
      */
    public boolean hasImageData() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'imageData' field.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder clearImageData() {
      imageData = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'items' field.
      * @return The value.
      */
    public java.util.List<com.ross.serializer.ItemAvro> getItems() {
      return items;
    }


    /**
      * Sets the value of the 'items' field.
      * @param value The value of 'items'.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder setItems(java.util.List<com.ross.serializer.ItemAvro> value) {
      validate(fields()[3], value);
      this.items = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'items' field has been set.
      * @return True if the 'items' field has been set, false otherwise.
      */
    public boolean hasItems() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'items' field.
      * @return This builder.
      */
    public com.ross.serializer.OrderAvro.Builder clearItems() {
      items = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public OrderAvro build() {
      try {
        OrderAvro record = new OrderAvro();
        record.orderId = fieldSetFlags()[0] ? this.orderId : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.shipping = fieldSetFlags()[1] ? this.shipping : (java.lang.Double) defaultValue(fields()[1]);
        record.imageData = fieldSetFlags()[2] ? this.imageData : (java.nio.ByteBuffer) defaultValue(fields()[2]);
        record.items = fieldSetFlags()[3] ? this.items : (java.util.List<com.ross.serializer.ItemAvro>) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<OrderAvro>
    WRITER$ = (org.apache.avro.io.DatumWriter<OrderAvro>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<OrderAvro>
    READER$ = (org.apache.avro.io.DatumReader<OrderAvro>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.orderId);

    out.writeDouble(this.shipping);

    if (this.imageData == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeBytes(this.imageData);
    }

    long size0 = this.items.size();
    out.writeArrayStart();
    out.setItemCount(size0);
    long actualSize0 = 0;
    for (com.ross.serializer.ItemAvro e0: this.items) {
      actualSize0++;
      out.startItem();
      e0.customEncode(out);
    }
    out.writeArrayEnd();
    if (actualSize0 != size0)
      throw new java.util.ConcurrentModificationException("Array-size written was " + size0 + ", but element count was " + actualSize0 + ".");

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.orderId = in.readString(this.orderId instanceof Utf8 ? (Utf8)this.orderId : null);

      this.shipping = in.readDouble();

      if (in.readIndex() != 1) {
        in.readNull();
        this.imageData = null;
      } else {
        this.imageData = in.readBytes(this.imageData);
      }

      long size0 = in.readArrayStart();
      java.util.List<com.ross.serializer.ItemAvro> a0 = this.items;
      if (a0 == null) {
        a0 = new SpecificData.Array<com.ross.serializer.ItemAvro>((int)size0, SCHEMA$.getField("items").schema());
        this.items = a0;
      } else a0.clear();
      SpecificData.Array<com.ross.serializer.ItemAvro> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<com.ross.serializer.ItemAvro>)a0 : null);
      for ( ; 0 < size0; size0 = in.arrayNext()) {
        for ( ; size0 != 0; size0--) {
          com.ross.serializer.ItemAvro e0 = (ga0 != null ? ga0.peek() : null);
          if (e0 == null) {
            e0 = new com.ross.serializer.ItemAvro();
          }
          e0.customDecode(in);
          a0.add(e0);
        }
      }

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.orderId = in.readString(this.orderId instanceof Utf8 ? (Utf8)this.orderId : null);
          break;

        case 1:
          this.shipping = in.readDouble();
          break;

        case 2:
          if (in.readIndex() != 1) {
            in.readNull();
            this.imageData = null;
          } else {
            this.imageData = in.readBytes(this.imageData);
          }
          break;

        case 3:
          long size0 = in.readArrayStart();
          java.util.List<com.ross.serializer.ItemAvro> a0 = this.items;
          if (a0 == null) {
            a0 = new SpecificData.Array<com.ross.serializer.ItemAvro>((int)size0, SCHEMA$.getField("items").schema());
            this.items = a0;
          } else a0.clear();
          SpecificData.Array<com.ross.serializer.ItemAvro> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<com.ross.serializer.ItemAvro>)a0 : null);
          for ( ; 0 < size0; size0 = in.arrayNext()) {
            for ( ; size0 != 0; size0--) {
              com.ross.serializer.ItemAvro e0 = (ga0 != null ? ga0.peek() : null);
              if (e0 == null) {
                e0 = new com.ross.serializer.ItemAvro();
              }
              e0.customDecode(in);
              a0.add(e0);
            }
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










