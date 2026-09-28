use std::fmt;

/// Fully resolved capture settings passed to the audio backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AudioCaptureOptions {
    pub sample_rate: u32,
    pub channels: u32,
    /// Flag sustained silence so the caller can auto-stop (tap mode).
    pub silence_detection: bool,
}

impl Default for AudioCaptureOptions {
    fn default() -> Self {
        Self {
            sample_rate: 16_000,
            channels: 1,
            silence_detection: false,
        }
    }
}

impl AudioCaptureOptions {
    /// Fill omitted options with the native addon's defaults, then validate.
    pub fn from_partial(
        partial: PartialAudioCaptureOptions,
    ) -> Result<Self, AudioCaptureConfigError> {
        let defaults = Self::default();
        let options = Self {
            sample_rate: partial.sample_rate.unwrap_or(defaults.sample_rate),
            channels: partial.channels.unwrap_or(defaults.channels),
            silence_detection: partial
                .silence_detection
                .unwrap_or(defaults.silence_detection),
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(self) -> Result<(), AudioCaptureConfigError> {
        if self.sample_rate == 0 || self.sample_rate > 192_000 {
            return Err(AudioCaptureConfigError::InvalidSampleRate(self.sample_rate));
        }
        if self.channels == 0 || self.channels > 2 {
            return Err(AudioCaptureConfigError::InvalidChannelCount(self.channels));
        }
        Ok(())
    }
}

/// Fields accepted by the native binding before applying defaults.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PartialAudioCaptureOptions {
    pub sample_rate: Option<u32>,
    pub channels: Option<u32>,
    pub silence_detection: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AudioCaptureConfigError {
    InvalidSampleRate(u32),
    InvalidChannelCount(u32),
}

impl fmt::Display for AudioCaptureConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSampleRate(rate) => {
                write!(
                    f,
                    "audio sample rate must be between 1 and 192000 Hz, got {rate}"
                )
            }
            Self::InvalidChannelCount(channels) => {
                write!(f, "audio capture supports 1 or 2 channels, got {channels}")
            }
        }
    }
}

impl std::error::Error for AudioCaptureConfigError {}
