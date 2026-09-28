# CUA image decode and capture limits

CUA's Rust image transforms reject PNG inputs before allocating a decoded
pixel buffer when the IHDR dimensions exceed **36,000,000 pixels** or
**32,768 pixels on either edge**. The decoder and caller-owned output buffer
are also limited to **128 MiB**. Errors name the limit that was exceeded.

The decode bound is applied to shared PNG-to-JPEG, resize, and crosshair
helpers; cursor-overlay's zoom crop; Linux Wayland's output-to-window crop;
and the macOS browser setup screenshot conversion. Direct core PNG decoders
check the header dimensions and `total_bytes()` before allocating. Paths using
`ImageReader` use the same pixel and edge preflight plus `image::Limits` for
decoder and output allocations.

The 7680x4320 8K UHD mode remains within the RGBA8 byte bound (33,177,600
pixels and 132,710,400 bytes). Captures within the limits remain at native
dimensions; the capture resize registry and coordinate mapping are unchanged.
A later operation that must decode a screenshot larger than these limits
returns a clear error; it does not decode a reduced preview first.

Native screen captures reject dimensions requiring more than 128 MiB of
RGBA/BGRA pixels before CUA-owned CPU buffers are allocated. Encoded captures
are capped at 128 MiB before being returned or expanded to base64. The limit
is checked in Windows GDI and WGC paths, Linux X11 and Wayland paths, the
macOS and portal file readers, and the GNOME helper response. Linux
image-tool and GNOME-helper stdout are read through bounded pipes.

These are per-capture bounds, not a process-wide or peak-working-set limit.
Operating-system, compositor, GPU, PipeWire, and image-tool internals may
allocate separately; concurrent captures, encoding buffers, base64 expansion,
and serialized JSON also add memory. This change does not establish or rule
out the cause of the separate Node/V8 crash.
