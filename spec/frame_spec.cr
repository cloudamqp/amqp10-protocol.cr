require "./spec_helper"

private def frame_bytes(type : UInt8, channel : UInt16, body : Bytes, doff : UInt8 = 2_u8) : Bytes
  io = IO::Memory.new
  extended = Bytes.new(doff.to_i * 4 - 8)
  io.write_bytes (8 + extended.size + body.size).to_u32, IO::ByteFormat::NetworkEndian
  io.write_byte doff
  io.write_byte type
  io.write_bytes channel, IO::ByteFormat::NetworkEndian
  io.write extended
  io.write body
  io.to_slice
end

describe AMQP10::Protocol::FrameReader do
  it "reads consecutive frames, skipping the extended header" do
    io = IO::Memory.new
    io.write frame_bytes(AMQP10::Protocol::AMQP_FRAME_TYPE, 3_u16, Bytes[1, 2, 3])
    io.write frame_bytes(AMQP10::Protocol::SASL_FRAME_TYPE, 0_u16, Bytes[4], doff: 3_u8)
    io.write frame_bytes(AMQP10::Protocol::AMQP_FRAME_TYPE, 0_u16, Bytes.empty)
    io.rewind
    reader = AMQP10::Protocol::FrameReader.new(io, 1024_u32)

    frame = reader.read
    frame.type.should eq AMQP10::Protocol::AMQP_FRAME_TYPE
    frame.channel.should eq 3_u16
    frame.body.should eq Bytes[1, 2, 3]
    frame.body_reader.gets_to_end.to_slice.should eq Bytes[1, 2, 3]

    frame = reader.read
    frame.type.should eq AMQP10::Protocol::SASL_FRAME_TYPE
    frame.body.should eq Bytes[4]

    reader.read.body.empty?.should be_true
  end

  it "never accepts less than the minimum max-frame-size" do
    AMQP10::Protocol::FrameReader.new(IO::Memory.new, 0_u32).max_frame_size.should eq AMQP10::Protocol::MIN_MAX_FRAME_SIZE
  end

  it "rejects frames larger than max-frame-size, counting the header" do
    max = AMQP10::Protocol::MIN_MAX_FRAME_SIZE
    reader = AMQP10::Protocol::FrameReader.new(IO::Memory.new(frame_bytes(0_u8, 0_u16, Bytes.new(max - 8 + 1))), max)
    expect_raises(AMQP10::Protocol::DecodeError, "AMQP 1.0 frame too large #{max + 1}") { reader.read }

    reader = AMQP10::Protocol::FrameReader.new(IO::Memory.new(frame_bytes(0_u8, 0_u16, Bytes.new(max - 8))), max)
    reader.read.body.size.should eq max - 8
  end

  it "lowers max-frame-size without growing past the buffer" do
    reader = AMQP10::Protocol::FrameReader.new(IO::Memory.new, 4096_u32)
    reader.max_frame_size = 1024_u32
    reader.max_frame_size.should eq 1024_u32
    reader.max_frame_size = 1_000_000_u32
    reader.max_frame_size.should eq 4096_u32
    reader.buffer_size.should eq 4096_u32
  end

  it "rejects invalid data offsets and sizes" do
    expect_raises(AMQP10::Protocol::DecodeError, /data offset/) do
      AMQP10::Protocol::FrameReader.new(IO::Memory.new(Bytes[0, 0, 0, 8, 1, 0, 0, 0]), 1024_u32).read
    end
    expect_raises(AMQP10::Protocol::DecodeError, /frame size/) do
      AMQP10::Protocol::FrameReader.new(IO::Memory.new(Bytes[0, 0, 0, 8, 4, 0, 0, 0]), 1024_u32).read
    end
  end
end

describe AMQP10::Protocol::FrameWriter do
  it "writes a performative frame readable by FrameReader" do
    io = IO::Memory.new
    AMQP10::Protocol::FrameWriter.write_performative(io, 7_u16, AMQP10::Protocol::AMQP_FRAME_TYPE,
      AMQP10::Protocol::Descriptor::DETACH, [AMQP10::Protocol::Value.uint(2_u32), AMQP10::Protocol::Value.bool(true)])
    io.rewind

    frame = AMQP10::Protocol::FrameReader.new(io, 1024_u32).read
    frame.channel.should eq 7_u16
    detach = AMQP10::Protocol::Detach.from_value(AMQP10::Protocol::Codec.decode(frame.body_reader))
    detach.handle.should eq 2_u32
    detach.closed.should be_true
  end
end
