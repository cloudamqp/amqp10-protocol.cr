require "./spec_helper"

describe IO::Memory do
  it "#reset points the IO at new bytes, from the start" do
    io = IO::Memory.new(Bytes[1, 2, 3])
    io.read_byte
    io.reset(Bytes[7, 8])
    io.pos.should eq 0
    io.size.should eq 2
    io.gets_to_end.to_slice.should eq Bytes[7, 8]
  end

  it "#reset takes over the writability of the bytes" do
    io = IO::Memory.new(Bytes.new(2))
    io.reset(Bytes.new(2, read_only: true))
    expect_raises(IO::Error) { io.write_byte 1_u8 }
    AMQP10::Protocol::Codec.read_slice(io, 1).read_only?.should be_true

    io.reset(Bytes.new(2))
    io.write_byte 1_u8
    io.rewind
    AMQP10::Protocol::Codec.read_slice(io, 1).read_only?.should be_false
  end

  it "#reset reopens a closed IO" do
    io = IO::Memory.new(Bytes[1])
    io.close
    io.reset(Bytes[2])
    io.closed?.should be_false
    io.read_byte.should eq 2
  end
end
