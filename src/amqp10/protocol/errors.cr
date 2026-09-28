module AMQP10::Protocol
  class Error < Exception; end

  class DecodeError < Error; end

  class ProtocolError < Error; end
end
