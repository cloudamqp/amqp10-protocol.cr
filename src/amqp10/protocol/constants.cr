module AMQP10::Protocol
  PROTOCOL_HEADER = Bytes['A'.ord.to_u8, 'M'.ord.to_u8, 'Q'.ord.to_u8, 'P'.ord.to_u8, 0_u8, 1_u8, 0_u8, 0_u8]
  SASL_HEADER     = Bytes['A'.ord.to_u8, 'M'.ord.to_u8, 'Q'.ord.to_u8, 'P'.ord.to_u8, 3_u8, 1_u8, 0_u8, 0_u8]

  AMQP_FRAME_TYPE = 0_u8
  SASL_FRAME_TYPE = 1_u8

  MIN_MAX_FRAME_SIZE =    512_u32
  DEFAULT_WINDOW     = 65_535_u32
end
