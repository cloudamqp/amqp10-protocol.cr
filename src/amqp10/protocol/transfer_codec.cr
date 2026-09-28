require "./codec"
require "./frame"
require "./performatives"

module AMQP10::Protocol
  # Encoders and allocation-free decoders for the performatives on the
  # per-message path: transfer, disposition and flow.
  module TransferCodec
    extend self

    record TransferView,
      handle : UInt32,
      delivery_id : UInt32?,
      delivery_tag : Bytes?,
      message_format : UInt32?,
      settled : Bool,
      more : Bool,
      aborted : Bool

    record DispositionView,
      role : Role,
      first : UInt32,
      last : UInt32?,
      settled : Bool,
      outcome : Outcome?,
      state_present : Bool

    # ameba:disable Metrics/CyclomaticComplexity
    def read_transfer(reader : IO::Memory) : TransferView
      descriptor = Codec.read_descriptor_code(reader)
      raise DecodeError.new("expected transfer") unless descriptor == Descriptor::TRANSFER
      count, end_pos = Codec.read_list_header(reader)
      handle = nil
      delivery_id = nil
      delivery_tag = nil
      message_format = nil
      settled = false
      more = false
      aborted = false

      index = 0
      while index < count
        case index
        when 0
          handle = read_uint32_value(reader, "transfer handle")
        when 1
          delivery_id = read_optional_uint(reader, "transfer delivery-id")
        when 2
          delivery_tag = read_optional_binary(reader)
        when 3
          message_format = read_optional_uint(reader, "transfer message-format")
        when 4
          settled = read_optional_bool(reader) || false
        when 5
          more = read_optional_bool(reader) || false
        when 9
          aborted = read_optional_bool(reader) || false
        else
          Codec.skip_value(reader)
        end
        index += 1
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      handle_value = handle
      raise DecodeError.new("transfer missing handle") unless handle_value
      TransferView.new(handle_value, delivery_id, delivery_tag, message_format, settled, more, aborted)
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated AMQP 1.0 transfer", cause: ex)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def read_disposition(reader : IO::Memory) : DispositionView
      descriptor = Codec.read_descriptor_code(reader)
      raise DecodeError.new("expected disposition") unless descriptor == Descriptor::DISPOSITION
      count, end_pos = Codec.read_list_header(reader)
      role = nil
      first = nil
      last = nil
      settled = false
      outcome = nil
      state_present = false

      index = 0
      while index < count
        case index
        when 0
          role = Codec.read_bool_value(reader) ? Role::Receiver : Role::Sender
        when 1
          first = read_uint32_value(reader, "disposition first")
        when 2
          last = read_optional_uint(reader, "disposition last")
        when 3
          settled = read_optional_bool(reader) || false
        when 4
          state_present, outcome = read_state(reader)
        else
          Codec.skip_value(reader)
        end
        index += 1
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      role_value = role
      first_value = first
      raise DecodeError.new("disposition missing role") unless role_value
      raise DecodeError.new("disposition missing first") unless first_value
      DispositionView.new(role_value, first_value, last, settled, outcome, state_present)
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated AMQP 1.0 disposition", cause: ex)
    end

    private def read_optional_uint(reader, field : String) : UInt32?
      return if peek_null(reader)
      read_uint32_value(reader, field)
    end

    private def read_uint32_value(reader, field : String) : UInt32
      value = Codec.read_uint_value(reader)
      if value > UInt32::MAX
        raise DecodeError.new("#{field} #{value} exceeds uint range")
      end
      value.to_u32
    end

    private def read_optional_bool(reader) : Bool?
      return if peek_null(reader)
      Codec.read_bool_value(reader)
    end

    private def read_optional_binary(reader) : Bytes?
      return if peek_null(reader)
      Codec.read_binary_value(reader)
    end

    private def peek_null(reader) : Bool
      # Peek without consuming, rather than reading-then-rewinding, to avoid
      # complicating the hot path with a rewind.
      if reader.bytesize - reader.pos > 0
        # Null is a single byte and never followed by payload.
        slice = reader.peek
        if slice[0] == 0x40_u8
          reader.skip(1)
          return true
        end
      end
      false
    end

    # Returns whether a delivery-state field was present (vs. a null placeholder)
    # and, if it was a recognized terminal outcome, which one. A non-terminal
    # state (e.g. received) reports present=true with a nil outcome so the
    # caller does not mistake it for acceptance.
    private def read_state(reader) : Tuple(Bool, Outcome?)
      return {false, nil} if peek_null(reader)
      descriptor = Codec.read_descriptor_code(reader)
      outcome = case descriptor
                when Descriptor::ACCEPTED then Outcome::Accepted
                when Descriptor::RELEASED then Outcome::Released
                when Descriptor::REJECTED then Outcome::Rejected
                when Descriptor::MODIFIED then Outcome::Modified
                end
      Codec.skip_value(reader)
      {true, outcome}
    end

    # Returns the number of bytes written.
    def write_disposition(io : IO, channel : UInt16, first : UInt32, outcome : Outcome, settled = true,
                          role : Role = Role::Receiver, last : UInt32? = nil) : UInt64
      state_size = outcome_size(outcome)
      last_size = last ? Codec.uint_size(last) : 1
      fields_size = 1 + Codec.uint_size(first) + last_size + 1 + state_size
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
      Codec.write_descriptor(io, Descriptor::DISPOSITION)
      Codec.write_list_header(io, fields_size, 5)
      io.write_byte(role.receiver? ? 0x41_u8 : 0x42_u8)
      Codec.write_uint(io, first)
      if last
        Codec.write_uint(io, last)
      else
        io.write_byte 0x40_u8
      end
      io.write_byte(settled ? 0x41_u8 : 0x42_u8)
      write_outcome(io, outcome)
      io.flush
      frame_size.to_u64
    end

    def write_flow(io : IO, channel : UInt16, next_incoming_id : UInt32, incoming_window : UInt32,
                   next_outgoing_id : UInt32, outgoing_window : UInt32, handle : UInt32? = nil,
                   delivery_count : UInt32? = nil, link_credit : UInt32? = nil, drain : Bool = false) : UInt64
      fields_size = Codec.uint_size(next_incoming_id) + Codec.uint_size(incoming_window) +
                    Codec.uint_size(next_outgoing_id) + Codec.uint_size(outgoing_window)
      fields_count = 4
      if handle
        fields_size += Codec.uint_size(handle) + Codec.uint_size(delivery_count || 0_u32) + Codec.uint_size(link_credit || 0_u32)
        fields_count = 7
        if drain
          # available (field 7) is encoded as null, drain (field 8) as a boolean
          fields_size += 1 + 1
          fields_count = 9
        end
      end
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
      Codec.write_descriptor(io, Descriptor::FLOW)
      Codec.write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, next_incoming_id)
      Codec.write_uint(io, incoming_window)
      Codec.write_uint(io, next_outgoing_id)
      Codec.write_uint(io, outgoing_window)
      if handle
        Codec.write_uint(io, handle)
        Codec.write_uint(io, delivery_count || 0_u32)
        Codec.write_uint(io, link_credit || 0_u32)
        if drain
          io.write_byte 0x40_u8 # available: null
          Codec.write_bool(io, true)
        end
      end
      io.flush
      frame_size.to_u64
    end

    def write_transfer_performative(io, handle, delivery_id, delivery_tag, more, settled) : Nil
      # fields: handle(0) delivery-id(1) delivery-tag(2) message-format(3) settled(4) more(5)
      fields_count = more ? 6 : (settled ? 5 : 4)
      fields_size = Codec.uint_size(handle) + Codec.uint_size(delivery_id) + Codec.binary_size(delivery_tag) + 1
      fields_size += 1 if settled || more # settled field (bool or null)
      fields_size += 1 if more            # more field
      Codec.write_descriptor(io, Descriptor::TRANSFER)
      Codec.write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, handle)
      Codec.write_uint(io, delivery_id)
      Codec.write_binary(io, delivery_tag)
      io.write_byte 0x43_u8 # message-format = 0
      if more
        io.write_byte(settled ? 0x41_u8 : 0x40_u8) # settled (null when unsettled)
        io.write_byte 0x41_u8                      # more = true
      elsif settled
        io.write_byte 0x41_u8 # settled = true
      end
    end

    def transfer_performative_size(handle, delivery_id, delivery_tag, more, settled) : Int32
      fields_size = Codec.uint_size(handle) + Codec.uint_size(delivery_id) + Codec.binary_size(delivery_tag) + 1
      fields_size += 1 if settled || more
      fields_size += 1 if more
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_continuation_transfer_performative(io, handle, more) : Nil
      fields_count = more ? 6 : 1
      fields_size = Codec.uint_size(handle)
      fields_size += 5 if more
      Codec.write_descriptor(io, Descriptor::TRANSFER)
      Codec.write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, handle)
      if more
        4.times { io.write_byte 0x40_u8 }
        io.write_byte 0x41_u8
      end
    end

    def continuation_transfer_performative_size(handle, more) : Int32
      fields_size = Codec.uint_size(handle)
      fields_size += 5 if more
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    # Every outcome we emit is a descriptor followed by an empty list (list0).
    OUTCOME_SIZE = 3 + 1

    private def outcome_size(outcome : Outcome) : Int32
      OUTCOME_SIZE
    end

    private def write_outcome(io, outcome : Outcome) : Nil
      case outcome
      in .accepted?
        Codec.write_descriptor(io, Descriptor::ACCEPTED)
      in .released?
        Codec.write_descriptor(io, Descriptor::RELEASED)
      in .rejected?
        Codec.write_descriptor(io, Descriptor::REJECTED)
      in .modified?
        Codec.write_descriptor(io, Descriptor::MODIFIED)
      end
      io.write_byte 0x45_u8
    end
  end
end
