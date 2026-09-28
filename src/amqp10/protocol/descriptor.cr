module AMQP10::Protocol
  module Descriptor
    SASL_MECHANISMS = 0x40_u64
    SASL_INIT       = 0x41_u64
    SASL_OUTCOME    = 0x44_u64

    OPEN        = 0x10_u64
    BEGIN       = 0x11_u64
    ATTACH      = 0x12_u64
    FLOW        = 0x13_u64
    TRANSFER    = 0x14_u64
    DISPOSITION = 0x15_u64
    DETACH      = 0x16_u64
    END         = 0x17_u64
    CLOSE       = 0x18_u64
    ERROR       = 0x1d_u64

    SOURCE = 0x28_u64
    TARGET = 0x29_u64

    ACCEPTED = 0x24_u64
    REJECTED = 0x25_u64
    RELEASED = 0x26_u64
    MODIFIED = 0x27_u64

    HEADER                 = 0x70_u64
    DELIVERY_ANNOTATIONS   = 0x71_u64
    MESSAGE_ANNOTATIONS    = 0x72_u64
    PROPERTIES             = 0x73_u64
    APPLICATION_PROPERTIES = 0x74_u64
    DATA                   = 0x75_u64
    AMQP_SEQUENCE          = 0x76_u64
    AMQP_VALUE             = 0x77_u64
    FOOTER                 = 0x78_u64
  end

  module ErrorCondition
    INTERNAL_ERROR          = "amqp:internal-error"
    NOT_FOUND               = "amqp:not-found"
    UNAUTHORIZED_ACCESS     = "amqp:unauthorized-access"
    DECODE_ERROR            = "amqp:decode-error"
    RESOURCE_LIMIT_EXCEEDED = "amqp:resource-limit-exceeded"
    NOT_ALLOWED             = "amqp:not-allowed"
    INVALID_FIELD           = "amqp:invalid-field"
    NOT_IMPLEMENTED         = "amqp:not-implemented"
    RESOURCE_LOCKED         = "amqp:resource-locked"
    RESOURCE_DELETED        = "amqp:resource-deleted"
    PRECONDITION_FAILED     = "amqp:precondition-failed"
    ILLEGAL_STATE           = "amqp:illegal-state"
  end

  enum Role
    Sender
    Receiver
  end

  enum Outcome
    Accepted
    Released
    Rejected
    Modified
  end
end
