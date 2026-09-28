use napi::bindgen_prelude::Buffer;
use napi::{Error, Result};
use napi_derive::napi;

use crate::{AudioCaptureError, NativeAudioCaptureBackend};

#[napi(object)]
pub struct JsAudioCaptureOptions {
    #[napi(js_name = "sampleRate")]
    pub sample_rate: Option<u32>,
    pub channels: Option<u32>,
    #[napi(js_name = "silenceDetection")]
    pub silence_detection: Option<bool>,
}

#[napi(js_name = "NativeAudioCaptureBackend")]
pub struct NodeAudioCaptureBackend {
    backend: NativeAudioCaptureBackend,
}

#[napi]
impl NodeAudioCaptureBackend {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            backend: NativeAudioCaptureBackend::new(),
        }
    }

    #[napi(js_name = "startRecording")]
    pub fn start_recording(&self, options: Option<JsAudioCaptureOptions>) -> Result<()> {
        let options = options.unwrap_or(JsAudioCaptureOptions {
            sample_rate: None,
            channels: None,
            silence_detection: None,
        });
        let partial = crate::PartialAudioCaptureOptions {
            sample_rate: options.sample_rate,
            channels: options.channels,
            silence_detection: options.silence_detection,
        };
        self.backend
            .start_recording_partial(partial)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "stopRecording")]
    pub fn stop_recording(&self) -> Result<Buffer> {
        self.backend
            .stop_recording()
            .map(Buffer::from)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "isRecording")]
    pub fn is_recording(&self) -> bool {
        self.backend.is_recording()
    }

    #[napi(js_name = "silenceDetected")]
    pub fn silence_detected(&self) -> bool {
        self.backend.silence_detected()
    }

    #[napi(js_name = "drainAudio")]
    pub fn drain_audio(&self) -> Buffer {
        self.backend.drain_audio().into()
    }

    #[napi(js_name = "audioLevel")]
    pub fn audio_level(&self) -> f64 {
        self.backend.audio_level()
    }

    #[napi(js_name = "microphoneAuthorizationStatus")]
    pub fn microphone_authorization_status(&self) -> String {
        self.backend
            .microphone_authorization_status()
            .as_str()
            .to_owned()
    }
}

#[napi(js_name = "getPlatformBackendName")]
pub fn get_platform_backend_name(platform: Option<String>) -> Result<String> {
    let platform = platform.unwrap_or_else(|| std::env::consts::OS.to_owned());
    crate::get_platform_backend_name(&platform)
        .map(|backend| backend.as_str().to_owned())
        .map_err(Error::from_reason)
}

fn to_napi_error(error: AudioCaptureError) -> Error {
    let message = match error {
        AudioCaptureError::AlreadyRecording => "Native audio capture is already recording.",
        AudioCaptureError::NotRecording => "Native audio capture is not recording.",
        AudioCaptureError::InvalidOptions => {
            "Native audio capture requires 1 or 2 channels and a sample rate."
        }
        AudioCaptureError::InputDeviceInitialization(_) => {
            "Native audio capture failed to initialize the input device."
        }
        AudioCaptureError::InputDeviceStart(_) => {
            "Native audio capture failed to start the input device."
        }
        AudioCaptureError::InputDeviceStop(_) => {
            "Native audio capture failed to stop the input device."
        }
        AudioCaptureError::EmptyAudio => "Native audio capture produced empty audio.",
        AudioCaptureError::PcmBufferAllocation => {
            "Native audio capture could not reserve its PCM buffer."
        }
    };
    Error::from_reason(message)
}
