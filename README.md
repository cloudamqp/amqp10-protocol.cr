# amqp10-protocol.cr

amqp10-protocol.cr is an [AMQP 1.0](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html) serialization library for Crystal. It is the AMQP 1.0 counterpart of [amq-protocol.cr](https://github.com/cloudamqp/amq-protocol.cr), and is used by [LavinMQ](https://github.com/cloudamqp/lavinmq).

It covers the wire level of the protocol: the type system, framing, performatives and the SASL descriptor codes. Connection, session and link state, and the mapping of messages to an application's own model, are left to the library user.

## Installation

Add the dependency to your `shard.yml`:

```yaml
dependencies:
  amqp10-protocol:
    github: cloudamqp/amqp10-protocol.cr
```

## Usage

```crystal
require "amqp10-protocol"

# Read frames off a socket, with a buffer sized for the negotiated max-frame-size
reader = AMQP10::Protocol::FrameReader.new(socket, 131_072_u32)
frame = reader.read
value = AMQP10::Protocol::Codec.decode(frame.body_reader)
open = AMQP10::Protocol::Open.from_value(value)

# Write a performative
open = AMQP10::Protocol::Open.new("my-container", nil, 131_072_u32)
AMQP10::Protocol::FrameWriter.write_frame_header(socket, open.frame_size, AMQP10::Protocol::AMQP_FRAME_TYPE, 0_u16)
open.write_body(socket)
socket.flush
```

The main entry points:

| Type | Purpose |
|------|---------|
| `Value`, `Codec` | The AMQP 1.0 type system: decode any value to a `Value` and encode it back, plus streaming readers (`read_list_header`, `read_descriptor_code`, `skip_value`, ...) that decode straight off the wire without allocating |
| `FrameReader`, `FrameWriter`, `Frame` | Framing, reusing one buffer for every frame |
| `Open`, `Begin`, `Attach`, `Flow`, `Detach`, `Source`, `Target`, `ErrorInfo` | Performatives and their fields |
| `TransferCodec` | Allocation-free encoding and decoding of `transfer`, `disposition` and `flow`, the performatives on the per-message path |
| `Descriptor`, `ErrorCondition` | Descriptor codes and the standard error conditions |

Decoded binary and string slices point into the frame buffer; copy them if they must outlive the next `FrameReader#read`. This library reopens `IO::Memory` to add `#reset(bytes)`, which lets a decoder reuse one `IO::Memory` across frames.

## Development

```sh
crystal spec
crystal tool format --check
```
