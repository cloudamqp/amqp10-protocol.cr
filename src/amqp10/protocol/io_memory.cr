class IO::Memory
  # Points this IO::Memory at `bytes` (read from the start), so a decoder can
  # reuse one instance across frames and messages instead of allocating a new
  # IO::Memory each time. Mirrors what `IO::Memory.new(bytes)` initializes.
  def reset(bytes : Bytes) : self
    @buffer = bytes.to_unsafe
    @bytesize = @capacity = bytes.bytesize
    @pos = 0
    @closed = false
    @resizeable = false
    # `@writeable` was renamed to `@writable` in Crystal 1.21.0
    {% if compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
      @writable = !bytes.read_only?
    {% else %}
      @writeable = !bytes.read_only?
    {% end %}
    self
  end
end
