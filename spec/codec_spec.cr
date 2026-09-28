require "./spec_helper"

describe AMQP10::Protocol::Codec do
  it "decodes one-byte signed integer values" do
    value = AMQP10::Protocol::Codec.decode(IO::Memory.new(Bytes[0x51_u8, 0xff_u8]))

    value.int?.should eq -1_i64

    value = AMQP10::Protocol::Codec.decode(IO::Memory.new(Bytes[0x54_u8, 0xff_u8]))
    value.int?.should eq -1_i64

    value = AMQP10::Protocol::Codec.decode(IO::Memory.new(Bytes[0x55_u8, 0xff_u8]))
    value.int?.should eq -1_i64
  end

  it "preserves float and double values" do
    io = IO::Memory.new
    AMQP10::Protocol::Codec.write_value(io, AMQP10::Protocol::Value.float(1.25_f32))
    value = AMQP10::Protocol::Codec.decode(IO::Memory.new(io.to_slice))
    value.float?.should eq 1.25_f32

    io.clear
    AMQP10::Protocol::Codec.write_value(io, AMQP10::Protocol::Value.double(1.5_f64))
    value = AMQP10::Protocol::Codec.decode(IO::Memory.new(io.to_slice))
    value.double?.should eq 1.5_f64
  end

  it "includes the constructor byte in array8 encoded size" do
    io = IO::Memory.new
    AMQP10::Protocol::Codec.write_value(io, AMQP10::Protocol::Value.array([AMQP10::Protocol::Value.symbol("PLAIN")]))

    io.to_slice.should eq Bytes[0xe0_u8, 0x08_u8, 0x01_u8, 0xa3_u8, 0x05_u8,
      0x50_u8, 0x4c_u8, 0x41_u8, 0x49_u8, 0x4e_u8]
  end

  it "rejects compound counts larger than the encoded payload" do
    payload = Bytes[0xd0_u8, 0_u8, 0_u8, 0_u8, 4_u8, 0x7f_u8, 0xff_u8, 0xff_u8, 0xff_u8]

    expect_raises(AMQP10::Protocol::DecodeError) do
      AMQP10::Protocol::Codec.decode(IO::Memory.new(payload))
    end
  end

  it "rejects a list element that overruns the list's own declared size" do
    payload = IO::Memory.new
    payload.write_byte 0xc0_u8                          # list8
    payload.write_byte 7_u8                             # size: count byte + 6 payload bytes
    payload.write_byte 1_u8                             # count: 1 element
    payload.write_byte 0xb1_u8                          # string32 constructor
    AMQP10::Protocol::Codec.write_u32(payload, 100_u32) # claims a 100-byte string,
    payload.write Bytes.new(100)                        # far larger than the list's own 6-byte payload
    # but still present in the surrounding buffer, so the read itself succeeds
    # and only the post-loop "did we overrun end_pos" check can catch it.

    expect_raises(AMQP10::Protocol::DecodeError, /overran declared size/) do
      AMQP10::Protocol::Codec.decode(IO::Memory.new(payload.to_slice))
    end
  end

  it "raises DecodeError for oversized variable-width values" do
    payload = IO::Memory.new
    payload.write_byte 0xb1_u8
    AMQP10::Protocol::Codec.write_u32(payload, UInt32::MAX)

    expect_raises(AMQP10::Protocol::DecodeError) do
      AMQP10::Protocol::Codec.decode(IO::Memory.new(payload.to_slice))
    end
  end

  it "rejects excessively nested described values" do
    payload = IO::Memory.new
    80.times do
      payload.write_byte 0x00_u8
      payload.write_byte 0x43_u8
    end
    payload.write_byte 0x40_u8

    expect_raises(AMQP10::Protocol::DecodeError) do
      AMQP10::Protocol::Codec.decode(IO::Memory.new(payload.to_slice))
    end
  end

  it "reads descriptor codes encoded numerically and symbolically" do
    io = IO::Memory.new
    AMQP10::Protocol::Codec.write_descriptor(io, AMQP10::Protocol::Descriptor::DATA)
    io.write_byte 0x00_u8
    AMQP10::Protocol::Codec.write_symbol(io, "amqp:data:binary")
    reader = IO::Memory.new(io.to_slice)

    AMQP10::Protocol::Codec.read_descriptor_code(reader).should eq AMQP10::Protocol::Descriptor::DATA
    AMQP10::Protocol::Codec.read_descriptor_code(reader).should eq AMQP10::Protocol::Descriptor::DATA
  end

  it "rejects unknown symbolic descriptors" do
    io = IO::Memory.new
    io.write_byte 0x00_u8
    AMQP10::Protocol::Codec.write_symbol(io, "example:unknown")

    expect_raises(AMQP10::Protocol::DecodeError, /unknown descriptor/) do
      AMQP10::Protocol::Codec.read_descriptor_code(IO::Memory.new(io.to_slice))
    end
  end

  it "skips values of every fixed and variable width without decoding them" do
    io = IO::Memory.new
    io.write_byte 0x94_u8 # decimal128
    io.write Bytes.new(16)
    AMQP10::Protocol::Codec.write_value(io, AMQP10::Protocol::Value.list([AMQP10::Protocol::Value.string("x" * 300)]))
    AMQP10::Protocol::Codec.write_value(io, AMQP10::Protocol::Value.described(
      AMQP10::Protocol::Value.ulong(1_u64), AMQP10::Protocol::Value.binary(Bytes.new(3))))
    io.write_byte 0x43_u8
    reader = IO::Memory.new(io.to_slice)

    3.times { AMQP10::Protocol::Codec.skip_value(reader) }
    AMQP10::Protocol::Codec.read_uint_value(reader).should eq 0_u64
    reader.pos.should eq reader.bytesize
  end

  it "skips every fixed-width type" do
    widths = {
      0x40 => 0, 0x41 => 0, 0x42 => 0, 0x43 => 0, 0x44 => 0, 0x45 => 0,
      0x50 => 1, 0x51 => 1, 0x52 => 1, 0x53 => 1, 0x54 => 1, 0x55 => 1, 0x56 => 1,
      0x60 => 2, 0x61 => 2,
      0x70 => 4, 0x71 => 4, 0x72 => 4, 0x73 => 4, 0x74 => 4,
      0x80 => 8, 0x81 => 8, 0x82 => 8, 0x83 => 8, 0x84 => 8,
      0x94 => 16, 0x98 => 16,
    }
    widths.each do |code, width|
      bytes = Bytes.new(1 + width + 1)
      bytes[0] = code.to_u8
      bytes[-1] = 0x40_u8 # a null after the value
      reader = IO::Memory.new(bytes)
      AMQP10::Protocol::Codec.skip_value(reader)
      reader.pos.should eq 1 + width
    end
  end

  it "reads list and map headers and validates them against the payload" do
    io = IO::Memory.new
    AMQP10::Protocol::Codec.write_value(io, AMQP10::Protocol::Value.list([AMQP10::Protocol::Value.bool(true), AMQP10::Protocol::Value.null]))
    reader = IO::Memory.new(io.to_slice)
    count, end_pos = AMQP10::Protocol::Codec.read_list_header(reader)
    count.should eq 2
    end_pos.should eq io.size

    io = IO::Memory.new
    AMQP10::Protocol::Codec.write_value(io, AMQP10::Protocol::Value.map([{AMQP10::Protocol::Value.string("k"), AMQP10::Protocol::Value.uint(1_u32)}]))
    count, end_pos = AMQP10::Protocol::Codec.read_map_header(IO::Memory.new(io.to_slice))
    count.should eq 2
    end_pos.should eq io.size

    expect_raises(AMQP10::Protocol::DecodeError, /exceeds remaining frame payload/) do
      AMQP10::Protocol::Codec.read_list_header(IO::Memory.new(Bytes[0xc0_u8, 0x10_u8, 0x01_u8]))
    end
  end

  it "reads binary and string values as views into the buffer" do
    io = IO::Memory.new
    AMQP10::Protocol::Codec.write_binary(io, "abc".to_slice)
    AMQP10::Protocol::Codec.write_string(io, "def")
    io.write_byte 0x40_u8
    reader = IO::Memory.new(io.to_slice)

    AMQP10::Protocol::Codec.read_binary_value(reader).should eq "abc".to_slice
    AMQP10::Protocol::Codec.read_string_value(reader).should eq "def"
    AMQP10::Protocol::Codec.read_string_value(reader).should be_nil
  end

  it "sizes binary and map headers as they are written" do
    {0, 255, 256}.each do |size|
      io = IO::Memory.new
      AMQP10::Protocol::Codec.write_binary_header(io, size.to_u64)
      io.size.should eq AMQP10::Protocol::Codec.binary_header_size(size.to_u64)
      AMQP10::Protocol::Codec.binary_size(Bytes.new(size)).should eq io.size + size
    end
    {10, 300}.each do |fields_size|
      io = IO::Memory.new
      AMQP10::Protocol::Codec.write_map_header(io, fields_size, 4)
      io.size.should eq AMQP10::Protocol::Codec.map_header_size(fields_size, 4)
    end
  end
end
