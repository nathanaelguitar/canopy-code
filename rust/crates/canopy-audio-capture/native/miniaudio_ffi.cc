#include <cstdint>
#include <new>

#define MINIAUDIO_IMPLEMENTATION
#include "miniaudio.h"

extern "C" {

typedef void (*CanopyAudioDataCallback)(
    void* user_data,
    const int16_t* samples,
    uint32_t frame_count);

struct CanopyAudioDevice {
  ma_device device;
  CanopyAudioDataCallback callback;
  void* user_data;
  bool initialized;
  bool started;
};

static void CaptureDataCallback(
    ma_device* device,
    void* output,
    const void* input,
    ma_uint32 frame_count) {
  (void)output;
  if (device == nullptr || input == nullptr) {
    return;
  }

  auto* state = static_cast<CanopyAudioDevice*>(device->pUserData);
  if (state != nullptr && state->callback != nullptr) {
    state->callback(
        state->user_data,
        static_cast<const int16_t*>(input),
        frame_count);
  }
}

int32_t canopy_audio_capture_create(
    uint32_t sample_rate,
    uint32_t channels,
    CanopyAudioDataCallback callback,
    void* user_data,
    CanopyAudioDevice** output) {
  if (output == nullptr || callback == nullptr || sample_rate == 0 ||
      sample_rate > 192000 || channels == 0 || channels > 2) {
    return MA_INVALID_ARGS;
  }
  *output = nullptr;

  auto* state = new (std::nothrow) CanopyAudioDevice{};
  if (state == nullptr) {
    return MA_OUT_OF_MEMORY;
  }
  state->callback = callback;
  state->user_data = user_data;

  ma_device_config config = ma_device_config_init(ma_device_type_capture);
  config.capture.format = ma_format_s16;
  config.capture.channels = channels;
  config.sampleRate = sample_rate;
  config.dataCallback = CaptureDataCallback;
  config.pUserData = state;

  const ma_result result = ma_device_init(nullptr, &config, &state->device);
  if (result != MA_SUCCESS) {
    delete state;
    return result;
  }
  state->initialized = true;
  *output = state;
  return MA_SUCCESS;
}

int32_t canopy_audio_capture_start(CanopyAudioDevice* state) {
  if (state == nullptr || !state->initialized || state->started) {
    return MA_INVALID_OPERATION;
  }
  const ma_result result = ma_device_start(&state->device);
  if (result == MA_SUCCESS) {
    state->started = true;
  }
  return result;
}

int32_t canopy_audio_capture_stop(CanopyAudioDevice* state) {
  if (state == nullptr || !state->initialized || !state->started) {
    return MA_INVALID_OPERATION;
  }
  const ma_result result = ma_device_stop(&state->device);
  state->started = false;
  return result;
}

void canopy_audio_capture_destroy(CanopyAudioDevice* state) {
  if (state == nullptr) {
    return;
  }
  if (state->initialized) {
    if (state->started) {
      (void)ma_device_stop(&state->device);
      state->started = false;
    }
    ma_device_uninit(&state->device);
    state->initialized = false;
  }
  delete state;
}

const char* canopy_audio_capture_result_description(int32_t result) {
  return ma_result_description(static_cast<ma_result>(result));
}

}  // extern "C"
