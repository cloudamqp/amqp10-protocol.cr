require "./spec_helper"

describe AMQP10::Protocol::TransferCodec do
  it "decodes transfer aborted from field 9" do
    {true, false}.each do |aborted|
      payload = IO::Memory.new
      fields = [
        AMQP10::Protocol::Value.uint(1_u32),
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.null,
        AMQP10::Protocol::Value.bool(aborted),
        AMQP10::Protocol::Value.bool(!aborted),
      ]
      AMQP10::Protocol::Codec.write_described_list(payload, AMQP10::Protocol::Descriptor::TRANSFER, fields)

      transfer = AMQP10::Protocol::TransferCodec.read_transfer(IO::Memory.new(payload.to_slice))

      transfer.aborted.should eq aborted
    end
  end

  it "writes mandatory session fields in flow frames" do
    io = IO::Memory.new

    written = AMQP10::Protocol::TransferCodec.write_flow(io, 3_u16, 11_u32, 22_u32, 33_u32, 44_u32,
      5_u32, 6_u32, 7_u32)

    written.should eq io.size
    bytes = io.to_slice
    frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
    reader = IO::Memory.new(bytes[8, frame_size.to_i - 8])
    flow = AMQP10::Protocol::Flow.from_value(AMQP10::Protocol::Codec.decode(reader))

    flow.next_incoming_id.should eq 11_u32
    flow.incoming_window.should eq 22_u32
    flow.next_outgoing_id.should eq 33_u32
    flow.outgoing_window.should eq 44_u32
    flow.handle.should eq 5_u32
    flow.delivery_count.should eq 6_u32
    flow.link_credit.should eq 7_u32
  end

  it "raises DecodeError for out-of-range uint fields" do
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    AMQP10::Protocol::Codec.write_ulong(payload, AMQP10::Protocol::Descriptor::TRANSFER)
    payload.write_byte 0xc0_u8
    payload.write_byte 10_u8
    payload.write_byte 1_u8
    payload.write_byte 0x80_u8
    AMQP10::Protocol::Codec.write_u64(payload, UInt64::MAX)

    expect_raises(AMQP10::Protocol::DecodeError) do
      AMQP10::Protocol::TransferCodec.read_transfer(IO::Memory.new(payload.to_slice))
    end
  end

  it "reports the bytes written for a disposition" do
    io = IO::Memory.new
    bytes = AMQP10::Protocol::TransferCodec.write_disposition(io, 0_u16, 300_u32,
      AMQP10::Protocol::Outcome::Accepted, true, AMQP10::Protocol::Role::Sender, 305_u32)
    bytes.should eq io.size
    frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, io.to_slice[0, 4])
    frame_size.should eq io.size
    disposition = AMQP10::Protocol::TransferCodec.read_disposition(IO::Memory.new(io.to_slice[8, io.size - 8]))
    disposition.role.should eq AMQP10::Protocol::Role::Sender
    disposition.first.should eq 300_u32
    disposition.last.should eq 305_u32
    disposition.settled.should be_true
  end

  it "reports a non-terminal delivery state without a terminal outcome" do
    received = AMQP10::Protocol::Value.described(
      AMQP10::Protocol::Value.ulong(0x23_u64), # amqp:received:list
      AMQP10::Protocol::Value.list(Array(AMQP10::Protocol::Value).new)    )
    fields = [
      AMQP10::Protocol::Value.bool(true),
      AMQP10::Protocol::Value.uint(5_u32),
      AMQP10::Protocol::Value.null,
      AMQP10::Protocol::Value.bool(false),
      received,
    ]
    payload = IO::Memory.new
    AMQP10::Protocol::Codec.write_described_list(payload, AMQP10::Protocol::Descriptor::DISPOSITION, fields)

    disposition = AMQP10::Protocol::TransferCodec.read_disposition(IO::Memory.new(payload.to_slice))

    disposition.outcome.should be_nil
    disposition.state_present.should be_true
  end

  it "distinguishes a bare settlement from an absent state" do
    fields = [
      AMQP10::Protocol::Value.bool(true),
      AMQP10::Protocol::Value.uint(5_u32),
      AMQP10::Protocol::Value.null,
      AMQP10::Protocol::Value.bool(true),
      AMQP10::Protocol::Value.null,
    ]
    payload = IO::Memory.new
    AMQP10::Protocol::Codec.write_described_list(payload, AMQP10::Protocol::Descriptor::DISPOSITION, fields)

    disposition = AMQP10::Protocol::TransferCodec.read_disposition(IO::Memory.new(payload.to_slice))

    disposition.outcome.should be_nil
    disposition.state_present.should be_false
    disposition.settled.should be_true
  end

  it "writes first and continuation transfer performatives of the advertised size" do
    tag = "tag".to_slice
    {true, false}.each do |more|
      {true, false}.each do |settled|
        io = IO::Memory.new
        AMQP10::Protocol::TransferCodec.write_transfer_performative(io, 1_u32, 300_u32, tag, more, settled)
        io.size.should eq AMQP10::Protocol::TransferCodec.transfer_performative_size(1_u32, 300_u32, tag, more, settled)
        transfer = AMQP10::Protocol::TransferCodec.read_transfer(IO::Memory.new(io.to_slice))
        transfer.handle.should eq 1_u32
        transfer.delivery_id.should eq 300_u32
        transfer.delivery_tag.should eq tag
        transfer.message_format.should eq 0_u32
        transfer.more.should eq more
        transfer.settled.should eq settled
      end

      io = IO::Memory.new
      AMQP10::Protocol::TransferCodec.write_continuation_transfer_performative(io, 1_u32, more)
      io.size.should eq AMQP10::Protocol::TransferCodec.continuation_transfer_performative_size(1_u32, more)
      transfer = AMQP10::Protocol::TransferCodec.read_transfer(IO::Memory.new(io.to_slice))
      transfer.delivery_id.should be_nil
      transfer.more.should eq more
    end
  end

  it "writes a drain flow with an echo-able link state" do
    io = IO::Memory.new
    AMQP10::Protocol::TransferCodec.write_flow(io, 0_u16, 1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 0_u32, drain: true)
    flow = AMQP10::Protocol::Flow.from_value(AMQP10::Protocol::Codec.decode(IO::Memory.new(io.to_slice[8, io.size - 8])))
    flow.drain.should be_true
    flow.link_credit.should eq 0_u32
    flow.available.should be_nil
  end

  it "decodes the fields of a modified outcome" do
    annotations = AMQP10::Protocol::Value.map([
      {AMQP10::Protocol::Value.symbol("x-opt-reason"), AMQP10::Protocol::Value.string("offline")},
    ])
    encoded_annotations = IO::Memory.new
    AMQP10::Protocol::Codec.write_value(encoded_annotations, annotations)
    modified = AMQP10::Protocol::Value.described(AMQP10::Protocol::Value.ulong(AMQP10::Protocol::Descriptor::MODIFIED),
      AMQP10::Protocol::Value.list([AMQP10::Protocol::Value.bool(true), AMQP10::Protocol::Value.bool(false), annotations]))
    fields = [AMQP10::Protocol::Value.bool(true), AMQP10::Protocol::Value.uint(5_u32), AMQP10::Protocol::Value.null,
              AMQP10::Protocol::Value.bool(true), modified]
    payload = IO::Memory.new
    AMQP10::Protocol::Codec.write_described_list(payload, AMQP10::Protocol::Descriptor::DISPOSITION, fields)

    disposition = AMQP10::Protocol::TransferCodec.read_disposition(IO::Memory.new(payload.to_slice))

    disposition.outcome.should eq AMQP10::Protocol::Outcome::Modified
    disposition.delivery_failed.should be_true
    disposition.undeliverable_here.should be_false
    disposition.message_annotations.should eq encoded_annotations.to_slice
  end

  it "decodes a modified outcome without fields" do
    modified = AMQP10::Protocol::Value.described(AMQP10::Protocol::Value.ulong(AMQP10::Protocol::Descriptor::MODIFIED),
      AMQP10::Protocol::Value.list(Array(AMQP10::Protocol::Value).new))
    fields = [AMQP10::Protocol::Value.bool(true), AMQP10::Protocol::Value.uint(5_u32), AMQP10::Protocol::Value.null,
              AMQP10::Protocol::Value.bool(true), modified]
    payload = IO::Memory.new
    AMQP10::Protocol::Codec.write_described_list(payload, AMQP10::Protocol::Descriptor::DISPOSITION, fields)

    disposition = AMQP10::Protocol::TransferCodec.read_disposition(IO::Memory.new(payload.to_slice))

    disposition.outcome.should eq AMQP10::Protocol::Outcome::Modified
    disposition.delivery_failed.should be_false
    disposition.message_annotations.should be_nil
  end

  it "round-trips every outcome through a disposition" do
    AMQP10::Protocol::Outcome.each do |outcome|
      io = IO::Memory.new
      AMQP10::Protocol::TransferCodec.write_disposition(io, 0_u16, 1_u32, outcome, settled: false)
      disposition = AMQP10::Protocol::TransferCodec.read_disposition(IO::Memory.new(io.to_slice[8, io.size - 8]))
      disposition.role.should eq AMQP10::Protocol::Role::Receiver
      disposition.outcome.should eq outcome
      disposition.settled.should be_false
      disposition.last.should be_nil
    end
  end
end
