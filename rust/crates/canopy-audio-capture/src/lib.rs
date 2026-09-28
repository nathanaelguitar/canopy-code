//! Native microphone capture with WAV, streaming PCM, and silence detection.

use std::ffi::{CStr, c_void};
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

pub mod config;
pub use config::{AudioCaptureConfigError, AudioCaptureOptions, PartialAudioCaptureOptions};

#[cfg(feature = "node-addon")]
mod node_addon;

const MAX_WAV_BYTES: usize = 10 * 1024 * 1024;
const MAX_PCM_SAMPLES: usize = (MAX_WAV_BYTES - 44) / std::mem::size_of::<i16>();
const SILENCE_THRESHOLD: f64 = 0.03 * 32768.0;
const SILENCE_DURATION_SECS: u64 = 2;

static RECORDING_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Microphone permission state exposed by the platform backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MicrophoneAuthorizationStatus {
    Granted,
    Denied,
    Prompt,
    Unknown,
}

impl MicrophoneAuthorizationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::Prompt => "prompt",
            Self::Unknown => "unknown",
        }
    }
}

/// The native audio backend associated with a supported operating system.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeAudioBackendName {
    CoreAudio,
    AlsaPulse,
    Wasapi,
}

impl NativeAudioBackendName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CoreAudio => "coreaudio",
            Self::AlsaPulse => "alsa-pulse",
            Self::Wasapi => "wasapi",
        }
    }
}

/// Error returned by the capture API.
#[derive(Debug)]
pub enum AudioCaptureError {
    AlreadyRecording,
    NotRecording,
    InvalidOptions,
    InputDeviceInitialization(String),
    InputDeviceStart(String),
    InputDeviceStop(String),
    EmptyAudio,
    PcmBufferAllocation,
}

impl fmt::Display for AudioCaptureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyRecording => f.write_str("Native audio capture is already recording."),
            Self::NotRecording => f.write_str("Native audio capture is not recording."),
            Self::InvalidOptions => {
                f.write_str("Native audio capture requires 1 or 2 channels and a sample rate.")
            }
            Self::InputDeviceInitialization(detail) => write!(
                f,
                "Native audio capture failed to initialize the input device. ({detail})"
            ),
            Self::InputDeviceStart(detail) => write!(
                f,
                "Native audio capture failed to start the input device. ({detail})"
            ),
            Self::InputDeviceStop(detail) => {
                write!(
                    f,
                    "Native audio capture failed to stop the input device. ({detail})"
                )
            }
            Self::EmptyAudio => f.write_str("Native audio capture produced empty audio."),
            Self::PcmBufferAllocation => {
                f.write_str("Native audio capture could not reserve its PCM buffer.")
            }
        }
    }
}

impl std::error::Error for AudioCaptureError {}

#[derive(Default)]
struct CaptureData {
    pcm: Vec<i16>,
    sample_rate: u32,
    channels: u32,
    silence_detection_enabled: bool,
    speech_started: bool,
    silent_frames: u64,
    silence_detected: bool,
    level: f64,
    callback_error: Option<String>,
}

struct CallbackContext {
    capture: Arc<Mutex<CaptureData>>,
    channels: u32,
}

struct BackendState {
    device: Option<NativeDevice>,
    callback_context: Option<Box<CallbackContext>>,
    capture: Arc<Mutex<CaptureData>>,
}

/// A safe owner for one native microphone capture stream.
///
/// The underlying miniaudio addon was process-global, so this API also allows
/// only one active stream across backend instances. Dropping the backend stops
/// and releases an active input device.
pub struct NativeAudioCaptureBackend {
    state: Mutex<BackendState>,
}

impl Default for NativeAudioCaptureBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeAudioCaptureBackend {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BackendState {
                device: None,
                callback_context: None,
                capture: Arc::new(Mutex::new(CaptureData::default())),
            }),
        }
    }

    /// Start capture using the supplied format. Defaults are 16 kHz, mono,
    /// without silence detection.
    pub fn start_recording(&self, options: AudioCaptureOptions) -> Result<(), AudioCaptureError> {
        options
            .validate()
            .map_err(|_| AudioCaptureError::InvalidOptions)?;

        let mut backend = lock_unpoisoned(&self.state);
        if backend.device.is_some() {
            return Err(AudioCaptureError::AlreadyRecording);
        }
        let reservation = RecordingReservation::acquire()?;

        {
            let mut capture = lock_unpoisoned(&backend.capture);
            let additional = MAX_PCM_SAMPLES.saturating_sub(capture.pcm.capacity());
            capture
                .pcm
                .try_reserve_exact(additional)
                .map_err(|_| AudioCaptureError::PcmBufferAllocation)?;
            capture.pcm.clear();
            capture.sample_rate = options.sample_rate;
            capture.channels = options.channels;
            capture.silence_detection_enabled = options.silence_detection;
            capture.speech_started = false;
            capture.silent_frames = 0;
            capture.silence_detected = false;
            capture.level = 0.0;
            capture.callback_error = None;
        }

        let callback_context = Box::new(CallbackContext {
            capture: Arc::clone(&backend.capture),
            channels: options.channels,
        });
        let user_data = (&*callback_context as *const CallbackContext)
            .cast_mut()
            .cast::<c_void>();
        let device = NativeDevice::new(options, capture_callback, user_data)
            .map_err(AudioCaptureError::InputDeviceInitialization)?;
        device
            .start()
            .map_err(AudioCaptureError::InputDeviceStart)?;

        backend.callback_context = Some(callback_context);
        backend.device = Some(device);
        reservation.commit();
        Ok(())
    }

    /// Start capture from optional settings, applying native binding defaults.
    pub fn start_recording_partial(
        &self,
        options: PartialAudioCaptureOptions,
    ) -> Result<(), AudioCaptureError> {
        let options = AudioCaptureOptions::from_partial(options)
            .map_err(|_| AudioCaptureError::InvalidOptions)?;
        self.start_recording(options)
    }

    /// Stop the current recording and return a little-endian 16-bit PCM WAV.
    pub fn stop_recording(&self) -> Result<Vec<u8>, AudioCaptureError> {
        let mut backend = lock_unpoisoned(&self.state);
        let device = backend
            .device
            .take()
            .ok_or(AudioCaptureError::NotRecording)?;
        let stop_result = device.stop();
        drop(device);
        backend.callback_context.take();
        RECORDING_ACTIVE.store(false, Ordering::Release);
        stop_result.map_err(AudioCaptureError::InputDeviceStop)?;

        let (pcm, sample_rate, channels) = {
            let mut capture = lock_unpoisoned(&backend.capture);
            (
                std::mem::take(&mut capture.pcm),
                capture.sample_rate,
                capture.channels,
            )
        };

        if pcm.is_empty() {
            return Err(AudioCaptureError::EmptyAudio);
        }

        Ok(to_wav(&pcm, sample_rate, channels))
    }

    pub fn is_recording(&self) -> bool {
        lock_unpoisoned(&self.state).device.is_some()
    }

    /// True once trailing silence was detected or the bounded buffer filled.
    pub fn silence_detected(&self) -> bool {
        let capture = Arc::clone(&lock_unpoisoned(&self.state).capture);
        lock_unpoisoned(&capture).silence_detected
    }

    /// Return and clear raw little-endian PCM collected since the previous call.
    pub fn drain_audio(&self) -> Vec<u8> {
        let capture = Arc::clone(&lock_unpoisoned(&self.state).capture);
        let mut capture = lock_unpoisoned(&capture);
        let mut bytes = Vec::with_capacity(capture.pcm.len() * 2);
        for sample in capture.pcm.drain(..) {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        bytes
    }

    /// Recent input level from 0.0 to 1.0, for a waveform display.
    pub fn audio_level(&self) -> f64 {
        let capture = Arc::clone(&lock_unpoisoned(&self.state).capture);
        lock_unpoisoned(&capture).level
    }

    /// Return and clear a panic caught inside the native audio callback.
    pub fn take_callback_error(&self) -> Option<String> {
        let capture = Arc::clone(&lock_unpoisoned(&self.state).capture);
        lock_unpoisoned(&capture).callback_error.take()
    }

    pub fn microphone_authorization_status(&self) -> MicrophoneAuthorizationStatus {
        microphone_authorization_status()
    }
}

impl Drop for NativeAudioCaptureBackend {
    fn drop(&mut self) {
        let backend = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(device) = backend.device.take() {
            drop(device);
            backend.callback_context.take();
            RECORDING_ACTIVE.store(false, Ordering::Release);
        }
    }
}

struct RecordingReservation {
    committed: bool,
}

impl RecordingReservation {
    fn acquire() -> Result<Self, AudioCaptureError> {
        RECORDING_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self { committed: false })
            .map_err(|_| AudioCaptureError::AlreadyRecording)
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for RecordingReservation {
    fn drop(&mut self) {
        if !self.committed {
            RECORDING_ACTIVE.store(false, Ordering::Release);
        }
    }
}

#[repr(C)]
struct NativeDeviceOpaque {
    _private: [u8; 0],
}

type NativeDataCallback = unsafe extern "C" fn(*mut c_void, *const i16, u32);

unsafe extern "C" {
    fn canopy_audio_capture_create(
        sample_rate: u32,
        channels: u32,
        callback: NativeDataCallback,
        user_data: *mut c_void,
        output: *mut *mut NativeDeviceOpaque,
    ) -> i32;
    fn canopy_audio_capture_start(device: *mut NativeDeviceOpaque) -> i32;
    fn canopy_audio_capture_stop(device: *mut NativeDeviceOpaque) -> i32;
    fn canopy_audio_capture_destroy(device: *mut NativeDeviceOpaque);
    fn canopy_audio_capture_result_description(result: i32) -> *const std::ffi::c_char;
}

struct NativeDevice(NonNull<NativeDeviceOpaque>);

// The pointer is only accessed while BackendState's mutex is held. miniaudio
// permits device control from a non-callback thread and synchronizes callbacks
// during stop/uninit.
unsafe impl Send for NativeDevice {}

impl NativeDevice {
    fn new(
        options: AudioCaptureOptions,
        callback: NativeDataCallback,
        user_data: *mut c_void,
    ) -> Result<Self, String> {
        let mut output = std::ptr::null_mut();
        let result = unsafe {
            canopy_audio_capture_create(
                options.sample_rate,
                options.channels,
                callback,
                user_data,
                &mut output,
            )
        };
        if result != 0 {
            return Err(native_result_detail(result));
        }
        NonNull::new(output)
            .map(Self)
            .ok_or_else(|| "miniaudio returned an empty device handle".to_owned())
    }

    fn start(&self) -> Result<(), String> {
        let result = unsafe { canopy_audio_capture_start(self.0.as_ptr()) };
        if result == 0 {
            Ok(())
        } else {
            Err(native_result_detail(result))
        }
    }

    fn stop(&self) -> Result<(), String> {
        let result = unsafe { canopy_audio_capture_stop(self.0.as_ptr()) };
        if result == 0 {
            Ok(())
        } else {
            Err(native_result_detail(result))
        }
    }
}

impl Drop for NativeDevice {
    fn drop(&mut self) {
        unsafe { canopy_audio_capture_destroy(self.0.as_ptr()) };
    }
}

fn native_result_detail(result: i32) -> String {
    let description = unsafe { canopy_audio_capture_result_description(result) };
    if description.is_null() {
        return format!("miniaudio error {result}");
    }
    let description = unsafe { CStr::from_ptr(description) }.to_string_lossy();
    format!("{description} (miniaudio error {result})")
}

unsafe extern "C" fn capture_callback(user_data: *mut c_void, input: *const i16, frame_count: u32) {
    if user_data.is_null() || input.is_null() || frame_count == 0 {
        return;
    }

    let callback_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = unsafe { &*user_data.cast::<CallbackContext>() };
        let Some(sample_count) = (frame_count as usize).checked_mul(context.channels as usize)
        else {
            return;
        };
        let samples = unsafe { std::slice::from_raw_parts(input, sample_count) };
        on_audio_data(&context.capture, samples, frame_count as u64);
    }));

    if callback_result.is_err() {
        let context = unsafe { &*user_data.cast::<CallbackContext>() };
        lock_unpoisoned(&context.capture).callback_error =
            Some("panic caught in native audio callback".to_owned());
    }
}

fn on_audio_data(capture: &Mutex<CaptureData>, samples: &[i16], frame_count: u64) {
    let sample_count = samples.len();
    let mean_abs = if sample_count == 0 {
        0.0
    } else {
        samples
            .iter()
            .map(|sample| f64::from(sample.unsigned_abs()))
            .sum::<f64>()
            / sample_count as f64
    };

    let mut capture = lock_unpoisoned(capture);
    if capture.silence_detection_enabled && !capture.silence_detected {
        if mean_abs >= SILENCE_THRESHOLD {
            capture.speech_started = true;
            capture.silent_frames = 0;
        } else if capture.speech_started {
            capture.silent_frames = capture.silent_frames.saturating_add(frame_count);
            let needed = u64::from(capture.sample_rate) * SILENCE_DURATION_SECS;
            if capture.silent_frames >= needed {
                capture.silence_detected = true;
            }
        }
    }

    capture.level = mean_abs / 32768.0;
    let remaining = MAX_PCM_SAMPLES.saturating_sub(capture.pcm.len());
    let to_copy = sample_count.min(remaining);
    capture.pcm.extend_from_slice(&samples[..to_copy]);
    if to_copy < sample_count {
        capture.silence_detected = true;
    }
}

fn to_wav(pcm: &[i16], sample_rate: u32, channels: u32) -> Vec<u8> {
    let data_bytes = std::mem::size_of_val(pcm) as u32;
    let block_align = (channels * 2) as u16;
    let byte_rate = sample_rate * u32::from(block_align);
    let mut wav = Vec::with_capacity(44 + data_bytes as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_bytes).to_le_bytes());
    wav.extend_from_slice(b"WAVE");
    wav.extend_from_slice(b"fmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&(channels as u16).to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&block_align.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_bytes.to_le_bytes());
    for sample in pcm {
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    wav
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Map an OS name to the platform backend advertised by the package.
pub fn get_platform_backend_name(platform: &str) -> Result<NativeAudioBackendName, String> {
    match platform {
        "macos" | "darwin" => Ok(NativeAudioBackendName::CoreAudio),
        "linux" => Ok(NativeAudioBackendName::AlsaPulse),
        "windows" | "win32" => Ok(NativeAudioBackendName::Wasapi),
        _ => Err(format!(
            "Native audio capture is not available for {platform}."
        )),
    }
}

pub fn current_platform_backend_name() -> Result<NativeAudioBackendName, String> {
    get_platform_backend_name(std::env::consts::OS)
}

#[cfg(target_os = "macos")]
fn microphone_authorization_status() -> MicrophoneAuthorizationStatus {
    use objc2_av_foundation::{AVAuthorizationStatus, AVCaptureDevice, AVMediaTypeAudio};
    use objc2_foundation::{NSOperatingSystemVersion, NSProcessInfo};

    // CoreAudio capture predates the TCC microphone gate. Keep the package's
    // mac_permission.mm behavior on older systems instead of calling the
    // AVFoundation authorization API where no permission state is required.
    let macos_10_14 = NSOperatingSystemVersion {
        majorVersion: 10,
        minorVersion: 14,
        patchVersion: 0,
    };
    if !NSProcessInfo::processInfo().isOperatingSystemAtLeastVersion(macos_10_14) {
        return MicrophoneAuthorizationStatus::Granted;
    }

    let audio_media_type = unsafe { AVMediaTypeAudio };
    let Some(audio_type) = audio_media_type else {
        return MicrophoneAuthorizationStatus::Unknown;
    };

    // This class query reads the process TCC status and does not prompt.
    let status = unsafe { AVCaptureDevice::authorizationStatusForMediaType(audio_type) };
    let status = match status {
        AVAuthorizationStatus::Authorized => 3,
        AVAuthorizationStatus::Restricted => 1,
        AVAuthorizationStatus::Denied => 2,
        AVAuthorizationStatus::NotDetermined => 0,
        _ => -1,
    };
    microphone_authorization_status_from_native_value(status)
}

fn microphone_authorization_status_from_native_value(status: i64) -> MicrophoneAuthorizationStatus {
    match status {
        3 => MicrophoneAuthorizationStatus::Granted,
        1 | 2 => MicrophoneAuthorizationStatus::Denied,
        0 => MicrophoneAuthorizationStatus::Prompt,
        _ => MicrophoneAuthorizationStatus::Unknown,
    }
}

#[cfg(not(target_os = "macos"))]
fn microphone_authorization_status() -> MicrophoneAuthorizationStatus {
    MicrophoneAuthorizationStatus::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture_data(silence_detection_enabled: bool) -> CaptureData {
        CaptureData {
            pcm: Vec::new(),
            sample_rate: 16_000,
            channels: 1,
            silence_detection_enabled,
            ..CaptureData::default()
        }
    }

    #[test]
    fn default_options_match_the_native_capture_defaults() {
        assert_eq!(
            AudioCaptureOptions::default(),
            AudioCaptureOptions {
                sample_rate: 16_000,
                channels: 1,
                silence_detection: false,
            }
        );
    }

    #[test]
    fn rejects_invalid_options_before_opening_a_device() {
        let backend = NativeAudioCaptureBackend::new();
        for options in [
            AudioCaptureOptions {
                sample_rate: 0,
                ..AudioCaptureOptions::default()
            },
            AudioCaptureOptions {
                sample_rate: 192_001,
                ..AudioCaptureOptions::default()
            },
            AudioCaptureOptions {
                channels: 0,
                ..AudioCaptureOptions::default()
            },
            AudioCaptureOptions {
                channels: 3,
                ..AudioCaptureOptions::default()
            },
        ] {
            assert!(matches!(
                backend.start_recording(options),
                Err(AudioCaptureError::InvalidOptions)
            ));
        }
        assert!(!backend.is_recording());
    }

    #[test]
    fn maps_supported_and_unsupported_platform_names() {
        assert_eq!(
            get_platform_backend_name("darwin").unwrap(),
            NativeAudioBackendName::CoreAudio
        );
        assert_eq!(
            get_platform_backend_name("linux").unwrap(),
            NativeAudioBackendName::AlsaPulse
        );
        assert_eq!(
            get_platform_backend_name("win32").unwrap(),
            NativeAudioBackendName::Wasapi
        );
        assert_eq!(
            get_platform_backend_name("freebsd").unwrap_err(),
            "Native audio capture is not available for freebsd."
        );
    }

    #[test]
    fn writes_a_pcm_wav_header_and_little_endian_samples() {
        let wav = to_wav(&[-32_768, 0, 32_767], 16_000, 1);
        assert_eq!(wav.len(), 50);
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(wav[4..8].try_into().unwrap()), 42);
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(&wav[12..16], b"fmt ");
        assert_eq!(u32::from_le_bytes(wav[16..20].try_into().unwrap()), 16);
        assert_eq!(u16::from_le_bytes(wav[20..22].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(wav[22..24].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(wav[24..28].try_into().unwrap()), 16_000);
        assert_eq!(u32::from_le_bytes(wav[28..32].try_into().unwrap()), 32_000);
        assert_eq!(u16::from_le_bytes(wav[32..34].try_into().unwrap()), 2);
        assert_eq!(u16::from_le_bytes(wav[34..36].try_into().unwrap()), 16);
        assert_eq!(&wav[36..40], b"data");
        assert_eq!(u32::from_le_bytes(wav[40..44].try_into().unwrap()), 6);
        assert_eq!(&wav[44..], &[0, 128, 0, 0, 255, 127]);
    }

    #[test]
    fn capture_buffer_is_bounded_and_reports_overflow() {
        let mut capture = capture_data(false);
        capture.pcm.reserve(MAX_PCM_SAMPLES);
        let shared = Mutex::new(capture);
        let samples = vec![123_i16; MAX_PCM_SAMPLES + 32];
        on_audio_data(&shared, &samples, samples.len() as u64);
        let capture = lock_unpoisoned(&shared);
        assert_eq!(capture.pcm.len(), MAX_PCM_SAMPLES);
        assert!(capture.silence_detected);
        assert!(capture.pcm.len() * 2 + 44 <= MAX_WAV_BYTES);
    }

    #[test]
    fn drain_audio_returns_little_endian_pcm_and_clears_the_stream() {
        let backend = NativeAudioCaptureBackend::new();
        lock_unpoisoned(&backend.state).capture = Arc::new(Mutex::new(CaptureData {
            pcm: vec![-32_768, 0, 32_767],
            ..CaptureData::default()
        }));

        assert_eq!(backend.drain_audio(), [0, 128, 0, 0, 255, 127]);
        assert!(backend.drain_audio().is_empty());
    }

    #[test]
    fn audio_level_reports_mean_absolute_amplitude_from_the_latest_callback() {
        let backend = NativeAudioCaptureBackend::new();
        let capture = Arc::clone(&lock_unpoisoned(&backend.state).capture);
        on_audio_data(&capture, &[-16_384, 8_192], 2);

        assert_eq!(backend.audio_level(), 0.375);
        on_audio_data(&capture, &[0, 0], 2);
        assert_eq!(backend.audio_level(), 0.0);
    }

    #[test]
    fn silence_detection_status_values_match_the_native_addon_contract() {
        assert_eq!(
            microphone_authorization_status_from_native_value(3),
            MicrophoneAuthorizationStatus::Granted
        );
        assert_eq!(
            microphone_authorization_status_from_native_value(2),
            MicrophoneAuthorizationStatus::Denied
        );
        assert_eq!(
            microphone_authorization_status_from_native_value(1),
            MicrophoneAuthorizationStatus::Denied
        );
        assert_eq!(
            microphone_authorization_status_from_native_value(0),
            MicrophoneAuthorizationStatus::Prompt
        );
        assert_eq!(
            microphone_authorization_status_from_native_value(4),
            MicrophoneAuthorizationStatus::Unknown
        );
    }

    #[test]
    fn silence_detection_ignores_leading_silence_then_flags_two_seconds() {
        let shared = Mutex::new(capture_data(true));
        let silence = [0_i16; 160];
        let speech = [1_000_i16; 160];

        for _ in 0..200 {
            on_audio_data(&shared, &silence, 160);
        }
        assert!(!lock_unpoisoned(&shared).silence_detected);

        on_audio_data(&shared, &speech, 160);
        for _ in 0..199 {
            on_audio_data(&shared, &silence, 160);
        }
        assert!(!lock_unpoisoned(&shared).silence_detected);

        on_audio_data(&shared, &silence, 160);
        assert!(lock_unpoisoned(&shared).silence_detected);
    }
}
