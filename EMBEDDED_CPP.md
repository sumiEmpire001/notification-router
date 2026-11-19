# Embedded C++ companion

Embedded notification quiet-hour decision engine with disabled-channel and monotonic UTC-minute fail-safe checks. Local minute must be supplied by a trusted timezone-aware host. No SQLite, DST conversion, retry delivery, provider integration or persistence.

## Host verification

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
python -m unittest discover -s tests -v
```

The C++11 header is allocation-free and uses no Arduino-specific APIs. Integrators may include `cpp/include/EmbeddedCore.h` in Arduino Uno or ESP32 sketches, but **firmware builds and physical-board tests are not yet verified**. Use advisory LEDs only; never directly switch mains, motors or safety-critical actuators. Handle sensor disconnection, time rollover, watchdog resets, retained configuration and electrical isolation separately. CI verifies host C++ and existing Python tests, not device behavior.
