require "./spec_helper"

private def decode_open(open : AMQP10::Protocol::Open) : AMQP10::Protocol::Open
  io = IO::Memory.new
  AMQP10::Protocol::FrameWriter.write_frame_header(io, open.frame_size, AMQP10::Protocol::AMQP_FRAME_TYPE, 0_u16)
  open.write_body(io)
  io.size.should eq open.frame_size
  io.rewind
  frame = AMQP10::Protocol::FrameReader.new(io, 1024_u32).read
  AMQP10::Protocol::Open.from_value(AMQP10::Protocol::Codec.decode(frame.body_reader))
end

describe AMQP10::Protocol::Open do
  it "round-trips with every combination of optional fields" do
    [nil, 2047_u16].each do |channel_max|
      [nil, 60_000_u32].each do |idle|
        [nil, "vhost:test"].each do |hostname|
          open = decode_open(AMQP10::Protocol::Open.new("container", hostname, 4096_u32, channel_max, idle))
          open.container_id.should eq "container"
          open.hostname.should eq hostname
          open.max_frame_size.should eq 4096_u32
          open.channel_max.should eq channel_max
          open.idle_time_out.should eq idle
        end
      end
    end
  end

  it "defaults an absent max-frame-size to no limit" do
    fields = [AMQP10::Protocol::Value.string("c")]
    value = AMQP10::Protocol::Value.described(AMQP10::Protocol::Value.ulong(AMQP10::Protocol::Descriptor::OPEN),
      AMQP10::Protocol::Value.list(fields))
    AMQP10::Protocol::Open.from_value(value).max_frame_size.should eq UInt32::MAX
  end
end

describe AMQP10::Protocol::Source do
  it "encodes the same bytes directly as through its Value form" do
    props = AMQP10::Protocol::Value.map([{AMQP10::Protocol::Value.symbol("k"), AMQP10::Protocol::Value.string("v")}])
    [AMQP10::Protocol::Source.new("/queues/q"),
     AMQP10::Protocol::Source.new(nil, durable: 2_u32, dynamic: true, dynamic_node_properties: props)].each do |source|
      direct = IO::Memory.new
      source.write_to(direct)
      direct.size.should eq source.encoded_size
      via_value = IO::Memory.new
      AMQP10::Protocol::Codec.write_value(via_value, source.to_value)
      direct.to_slice.should eq via_value.to_slice

      decoded = AMQP10::Protocol::Source.from_value(AMQP10::Protocol::Codec.decode(IO::Memory.new(direct.to_slice))).not_nil!
      decoded.address.should eq source.address
      decoded.durable.should eq source.durable
      decoded.dynamic.should eq source.dynamic
    end
  end
end

describe AMQP10::Protocol::Target do
  it "encodes the same bytes directly as through its Value form" do
    target = AMQP10::Protocol::Target.new("/exchanges/amq.topic/a.b", dynamic: false)
    direct = IO::Memory.new
    target.write_to(direct)
    direct.size.should eq target.encoded_size
    via_value = IO::Memory.new
    AMQP10::Protocol::Codec.write_value(via_value, target.to_value)
    direct.to_slice.should eq via_value.to_slice
  end
end

describe AMQP10::Protocol::ErrorInfo do
  it "encodes the same bytes directly as through its Value form" do
    [AMQP10::Protocol::ErrorInfo.new(AMQP10::Protocol::ErrorCondition::NOT_FOUND),
     AMQP10::Protocol::ErrorInfo.new(AMQP10::Protocol::ErrorCondition::NOT_FOUND, "no queue")].each do |error|
      direct = IO::Memory.new
      error.write_to(direct)
      direct.size.should eq error.encoded_size
      via_value = IO::Memory.new
      AMQP10::Protocol::Codec.write_value(via_value, error.to_value)
      direct.to_slice.should eq via_value.to_slice
    end
  end
end

describe AMQP10::Protocol::Attach do
  it "decodes role, settle modes, termini and initial delivery count" do
    fields = [
      AMQP10::Protocol::Value.string("link"),
      AMQP10::Protocol::Value.uint(4_u32),
      AMQP10::Protocol::Value.bool(true),
      AMQP10::Protocol::Value.ubyte(1_u8),
      AMQP10::Protocol::Value.ubyte(1_u8),
      AMQP10::Protocol::Source.new("/queues/q").to_value,
      AMQP10::Protocol::Value.null,
      AMQP10::Protocol::Value.null,
      AMQP10::Protocol::Value.null,
      AMQP10::Protocol::Value.uint(9_u32),
    ]
    value = AMQP10::Protocol::Value.described(AMQP10::Protocol::Value.ulong(AMQP10::Protocol::Descriptor::ATTACH),
      AMQP10::Protocol::Value.list(fields))
    attach = AMQP10::Protocol::Attach.from_value(value)
    attach.name.should eq "link"
    attach.handle.should eq 4_u32
    attach.role.should eq AMQP10::Protocol::Role::Receiver
    attach.snd_settle_mode.should eq 1_u8
    attach.rcv_settle_mode.should eq 1_u8
    attach.source.not_nil!.address.should eq "/queues/q"
    attach.target.should be_nil
    attach.initial_delivery_count.should eq 9_u32
  end

  it "requires name and handle" do
    value = AMQP10::Protocol::Value.described(AMQP10::Protocol::Value.ulong(AMQP10::Protocol::Descriptor::ATTACH),
      AMQP10::Protocol::Value.list([AMQP10::Protocol::Value.string("link")]))
    expect_raises(AMQP10::Protocol::DecodeError, /handle/) { AMQP10::Protocol::Attach.from_value(value) }
  end
end

describe AMQP10::Protocol::Begin do
  it "defaults the windows when absent" do
    value = AMQP10::Protocol::Value.described(AMQP10::Protocol::Value.ulong(AMQP10::Protocol::Descriptor::BEGIN),
      AMQP10::Protocol::Value.list([AMQP10::Protocol::Value.ushort(3_u16)]))
    begin_frame = AMQP10::Protocol::Begin.from_value(value)
    begin_frame.remote_channel.should eq 3_u16
    begin_frame.incoming_window.should eq AMQP10::Protocol::DEFAULT_WINDOW
    begin_frame.outgoing_window.should eq AMQP10::Protocol::DEFAULT_WINDOW
  end
end
