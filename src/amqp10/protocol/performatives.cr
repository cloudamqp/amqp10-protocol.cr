require "./codec"

module AMQP10::Protocol
  record ErrorInfo, condition : String, description : String? = nil do
    # Generic Value form, for FrameWriter.write_performative; write_to and
    # encoded_size below encode it directly without allocating.
    def to_value : Value
      fields = Array(Value).new(2)
      fields << Value.symbol(condition)
      if desc = description
        fields << Value.string(desc)
      end
      Value.described(Value.ulong(Descriptor::ERROR), Value.list(fields))
    end

    def encoded_size : Int32
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_to(io : IO) : Nil
      count = 1
      count = 2 if description
      Codec.write_descriptor(io, Descriptor::ERROR)
      Codec.write_list_header(io, fields_size, count)
      Codec.write_symbol(io, condition)
      if desc = description
        Codec.write_string(io, desc)
      end
    end

    private def fields_size : Int32
      size = Codec.string_size(condition)
      if desc = description
        size += Codec.string_size(desc)
      end
      size
    end
  end

  # A multiple field (1.6.11) holds either a single symbol or an array of them
  # :nodoc:
  def self.multiple_symbols(value : Value?) : Array(String)
    if list = value.try(&.list?)
      list.compact_map(&.symbol?)
    elsif symbol = value.try(&.symbol?)
      [symbol]
    else
      [] of String
    end
  end

  record Source,
    address : String?,
    durable : UInt32 = 0_u32,
    dynamic : Bool = false,
    dynamic_node_properties : Value? = nil,
    filter : Value? = nil,
    expiry_policy : String? = nil,
    capabilities : Array(String) = [] of String do
    def self.from_value(value : Value?) : Source?
      return unless value
      described = value.described? || return
      return unless described.descriptor_code? == Descriptor::SOURCE
      fields = described.value.list? || Array(Value).new
      address = fields[0]?.try &.string_like?
      durable = fields[1]?.try(&.uint?).try(&.to_u32) || 0_u32
      expiry_policy = fields[2]?.try &.symbol?
      dynamic = fields[4]?.try(&.bool?) || false
      dynamic_node_properties = fields[5]?.try { |v| v.null? ? nil : v }
      filter = fields[7]?.try { |v| v.null? ? nil : v }
      capabilities = Protocol.multiple_symbols(fields[10]?)
      new(address, durable, dynamic, dynamic_node_properties, filter, expiry_policy, capabilities)
    end

    # Generic Value form, e.g. for building an Attach with
    # FrameWriter.write_performative; write_to and encoded_size below encode it
    # directly without allocating. The filter is not encoded.
    def to_value : Value
      fields = Array(Value).new(field_count)
      fields << (address.try { |a| Value.string(a) } || Value.null)
      fields << (durable.zero? ? Value.null : Value.uint(durable))
      fields << (expiry_policy.try { |p| Value.symbol(p) } || Value.null)
      fields << Value.null
      fields << (dynamic ? Value.bool(true) : Value.null)
      if field_count > 5
        fields << (dynamic_node_properties || Value.null)
      end
      unless capabilities.empty?
        4.times { fields << Value.null } # distribution-mode, filter, default-outcome, outcomes
        fields << Value.array(capability_values)
      end
      Value.described(Value.ulong(Descriptor::SOURCE), Value.list(fields))
    end

    def encoded_size : Int32
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_to(io : IO) : Nil
      Codec.write_descriptor(io, Descriptor::SOURCE)
      Codec.write_list_header(io, fields_size, field_count)
      Codec.write_nullable_string(io, address)
      if durable.zero?
        io.write_byte 0x40_u8
      else
        Codec.write_uint(io, durable)
      end
      if policy = expiry_policy
        Codec.write_symbol(io, policy)
      else
        io.write_byte 0x40_u8
      end
      io.write_byte 0x40_u8 # timeout: null
      io.write_byte(dynamic ? 0x41_u8 : 0x40_u8)
      if field_count > 5
        if props = dynamic_node_properties
          Codec.write_value(io, props)
        else
          io.write_byte 0x40_u8
        end
      end
      unless capabilities.empty?
        4.times { io.write_byte 0x40_u8 } # distribution-mode, filter, default-outcome, outcomes
        Codec.write_array(io, capability_values)
      end
    end

    # Trailing null fields are left out
    private def field_count : Int32
      return 11 unless capabilities.empty?
      dynamic_node_properties ? 6 : 5
    end

    private def fields_size : Int32
      size = Codec.nullable_string_size(address)
      size += durable.zero? ? 1 : Codec.uint_size(durable)
      size += expiry_policy.try { |p| Codec.encoded_size(Value.symbol(p)) } || 1
      size += 1 + 1 # timeout, dynamic
      if field_count > 5
        size += dynamic_node_properties.try { |props| Codec.encoded_size(props) } || 1
      end
      unless capabilities.empty?
        size += 4 + Codec.encoded_size(Value.array(capability_values))
      end
      size
    end

    private def capability_values : Array(Value)
      capabilities.map { |capability| Value.symbol(capability) }
    end
  end

  record Target,
    address : String?,
    durable : UInt32 = 0_u32,
    dynamic : Bool = false,
    dynamic_node_properties : Value? = nil do
    def self.from_value(value : Value?) : Target?
      return unless value
      described = value.described? || return
      return unless described.descriptor_code? == Descriptor::TARGET
      fields = described.value.list? || Array(Value).new
      address = fields[0]?.try &.string_like?
      durable = fields[1]?.try(&.uint?).try(&.to_u32) || 0_u32
      dynamic = fields[4]?.try(&.bool?) || false
      dynamic_node_properties = fields[5]?.try { |v| v.null? ? nil : v }
      new(address, durable, dynamic, dynamic_node_properties)
    end

    # Generic Value form, e.g. for building an Attach with
    # FrameWriter.write_performative; write_to and encoded_size below encode it
    # directly without allocating.
    def to_value : Value
      fields = Array(Value).new(dynamic_node_properties ? 6 : 5)
      fields << (address.try { |a| Value.string(a) } || Value.null)
      fields << (durable.zero? ? Value.null : Value.uint(durable))
      fields << Value.null
      fields << Value.null
      fields << (dynamic ? Value.bool(true) : Value.null)
      if props = dynamic_node_properties
        fields << props
      end
      Value.described(Value.ulong(Descriptor::TARGET), Value.list(fields))
    end

    def encoded_size : Int32
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_to(io : IO) : Nil
      Codec.write_descriptor(io, Descriptor::TARGET)
      Codec.write_list_header(io, fields_size, dynamic_node_properties ? 6 : 5)
      Codec.write_nullable_string(io, address)
      if durable.zero?
        io.write_byte 0x40_u8
      else
        Codec.write_uint(io, durable)
      end
      io.write_byte 0x40_u8 # expiry-policy: null
      io.write_byte 0x40_u8 # timeout: null
      io.write_byte(dynamic ? 0x41_u8 : 0x40_u8)
      if props = dynamic_node_properties
        Codec.write_value(io, props)
      end
    end

    private def fields_size : Int32
      size = Codec.nullable_string_size(address)
      size += durable.zero? ? 1 : Codec.uint_size(durable)
      size += 1 + 1 + 1 # expiry-policy, timeout, dynamic
      if props = dynamic_node_properties
        size += Codec.encoded_size(props)
      end
      size
    end
  end

  # The target of a link to a transaction coordinator (4.5.1), attached in
  # place of a Target to declare and discharge transactions.
  record Coordinator, capabilities : Array(String) = [] of String do
    SYMBOLIC_DESCRIPTOR = "amqp:coordinator:list"

    def self.from_value(value : Value?) : Coordinator?
      return unless value
      described = value.described? || return
      descriptor = described.descriptor
      return unless descriptor.uint? == Descriptor::COORDINATOR || descriptor.symbol? == SYMBOLIC_DESCRIPTOR
      fields = described.value.list? || Array(Value).new
      new(Protocol.multiple_symbols(fields[0]?))
    end

    def to_value : Value
      fields = [Value.array(capability_values)]
      Value.described(Value.ulong(Descriptor::COORDINATOR), Value.list(fields))
    end

    def encoded_size : Int32
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_to(io : IO) : Nil
      Codec.write_descriptor(io, Descriptor::COORDINATOR)
      Codec.write_list_header(io, fields_size, 1)
      Codec.write_array(io, capability_values)
    end

    private def fields_size : Int32
      Codec.encoded_size(Value.array(capability_values))
    end

    private def capability_values : Array(Value)
      capabilities.map { |capability| Value.symbol(capability) }
    end
  end

  record Open,
    container_id : String,
    hostname : String?,
    max_frame_size : UInt32 = UInt32::MAX,
    channel_max : UInt16? = nil,
    idle_time_out : UInt32? = nil do
    def self.from_value(value : Value) : Open
      described = value.described? || raise DecodeError.new("expected open")
      raise DecodeError.new("expected open") unless described.descriptor_code? == Descriptor::OPEN
      fields = described.value.list? || raise DecodeError.new("open fields must be list")
      container_id = fields[0]?.try(&.string_like?) || ""
      hostname = fields[1]?.try &.string_like?
      # absent means no limit (the spec default)
      max_frame_size = fields[2]?.try(&.uint?).try(&.to_u32) || UInt32::MAX
      channel_max = fields[3]?.try(&.uint?).try { |v| v > UInt16::MAX ? UInt16::MAX : v.to_u16 }
      idle_time_out = fields[4]?.try(&.uint?).try(&.to_u32) # milliseconds
      new(container_id, hostname, max_frame_size, channel_max, idle_time_out)
    end

    def frame_size : UInt32
      (8 + 3 + Codec.list_header_size(fields_size) + fields_size).to_u32
    end

    # Writes the performative (everything after the frame header).
    def write_body(io : IO) : Nil
      count = field_count
      Codec.write_descriptor(io, Descriptor::OPEN)
      Codec.write_list_header(io, fields_size, count)
      Codec.write_string(io, container_id)
      Codec.write_nullable_string(io, hostname)
      Codec.write_uint(io, max_frame_size)
      if count > 3
        if cm = channel_max
          io.write_byte 0x60_u8
          Codec.write_u16(io, cm)
        else
          io.write_byte 0x40_u8
        end
      end
      if idle = idle_time_out
        Codec.write_uint(io, idle)
      end
    end

    private def field_count : Int32
      if idle_time_out
        5
      elsif channel_max
        4
      else
        3
      end
    end

    private def fields_size : Int32
      size = Codec.string_size(container_id) + Codec.nullable_string_size(hostname) + Codec.uint_size(max_frame_size)
      size += (channel_max ? 3 : 1) if field_count > 3
      if idle = idle_time_out
        size += Codec.uint_size(idle)
      end
      size
    end
  end

  record Begin,
    remote_channel : UInt16? = nil,
    next_outgoing_id : UInt32 = 0_u32,
    incoming_window : UInt32 = DEFAULT_WINDOW,
    outgoing_window : UInt32 = DEFAULT_WINDOW do
    def self.from_value(value : Value) : Begin
      described = value.described? || raise DecodeError.new("expected begin")
      raise DecodeError.new("expected begin") unless described.descriptor_code? == Descriptor::BEGIN
      fields = described.value.list? || Array(Value).new
      remote_channel = fields[0]?.try(&.uint?).try(&.to_u16)
      next_outgoing_id = fields[1]?.try(&.uint?).try(&.to_u32) || 0_u32
      incoming_window = fields[2]?.try(&.uint?).try(&.to_u32) || DEFAULT_WINDOW
      outgoing_window = fields[3]?.try(&.uint?).try(&.to_u32) || DEFAULT_WINDOW
      new(remote_channel, next_outgoing_id, incoming_window, outgoing_window)
    end
  end

  record Attach,
    name : String,
    handle : UInt32,
    role : Role,
    snd_settle_mode : UInt8? = nil,
    rcv_settle_mode : UInt8? = nil,
    source : Source? = nil,
    target : Target? = nil,
    initial_delivery_count : UInt32? = nil,
    coordinator : Coordinator? = nil do
    def self.from_value(value : Value) : Attach
      described = value.described? || raise DecodeError.new("expected attach")
      raise DecodeError.new("expected attach") unless described.descriptor_code? == Descriptor::ATTACH
      fields = described.value.list? || raise DecodeError.new("attach fields must be list")
      name = fields[0]?.try(&.string_like?) || raise DecodeError.new("attach missing name")
      handle = fields[1]?.try(&.uint?).try(&.to_u32) || raise DecodeError.new("attach missing handle")
      role_bool = fields[2]?.try(&.bool?)
      role = role_bool ? Role::Receiver : Role::Sender
      snd_settle_mode = fields[3]?.try(&.uint?).try(&.to_u8)
      rcv_settle_mode = fields[4]?.try(&.uint?).try(&.to_u8)
      source = Source.from_value(fields[5]?)
      target = Target.from_value(fields[6]?)
      coordinator = Coordinator.from_value(fields[6]?)
      initial_delivery_count = fields[9]?.try(&.uint?).try(&.to_u32)
      new(name, handle, role, snd_settle_mode, rcv_settle_mode, source, target, initial_delivery_count, coordinator)
    end
  end

  record Flow,
    next_incoming_id : UInt32?,
    incoming_window : UInt32?,
    next_outgoing_id : UInt32?,
    outgoing_window : UInt32?,
    handle : UInt32?,
    delivery_count : UInt32?,
    link_credit : UInt32?,
    available : UInt32?,
    drain : Bool,
    echo : Bool do
    def self.from_value(value : Value) : Flow
      described = value.described? || raise DecodeError.new("expected flow")
      raise DecodeError.new("expected flow") unless described.descriptor_code? == Descriptor::FLOW
      fields = described.value.list? || Array(Value).new
      new(
        fields[0]?.try(&.uint?).try(&.to_u32),
        fields[1]?.try(&.uint?).try(&.to_u32),
        fields[2]?.try(&.uint?).try(&.to_u32),
        fields[3]?.try(&.uint?).try(&.to_u32),
        fields[4]?.try(&.uint?).try(&.to_u32),
        fields[5]?.try(&.uint?).try(&.to_u32),
        fields[6]?.try(&.uint?).try(&.to_u32),
        fields[7]?.try(&.uint?).try(&.to_u32),
        fields[8]?.try(&.bool?) || false,
        fields[9]?.try(&.bool?) || false,
      )
    end
  end

  record Detach, handle : UInt32, closed : Bool do
    def self.from_value(value : Value) : Detach
      described = value.described? || raise DecodeError.new("expected detach")
      raise DecodeError.new("expected detach") unless described.descriptor_code? == Descriptor::DETACH
      fields = described.value.list? || raise DecodeError.new("detach fields must be list")
      handle = fields[0]?.try(&.uint?).try(&.to_u32) || raise DecodeError.new("detach missing handle")
      closed = fields[1]?.try(&.bool?) || false
      new(handle, closed)
    end
  end
end
