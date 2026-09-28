//! Streaming request bodies with upload progress for daemon file uploads.

use futures_util::stream;

const PROGRESS_CHUNK_BYTES: usize = 64 * 1024;

/// Number of request-body bytes yielded to the HTTP transport so far.
///
/// Progress is reported as the transport consumes body chunks. It does not
/// mean the daemon has received or persisted those bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UploadProgress {
    pub loaded: u64,
    pub total: u64,
}

pub(crate) fn body_with_progress<F>(data: Vec<u8>, on_progress: F) -> reqwest::Body
where
    F: FnMut(UploadProgress) + Send + 'static,
{
    let total = data.len() as u64;
    let stream = stream::unfold(
        (data.into_iter(), 0_u64, false, on_progress),
        move |(mut remaining, mut loaded, mut reported_initial, mut on_progress)| async move {
            if !reported_initial {
                reported_initial = true;
                on_progress(UploadProgress { loaded, total });
                return Some((
                    Ok::<Vec<u8>, std::io::Error>(Vec::new()),
                    (remaining, loaded, reported_initial, on_progress),
                ));
            }

            let mut chunk = Vec::with_capacity(PROGRESS_CHUNK_BYTES);
            while chunk.len() < PROGRESS_CHUNK_BYTES {
                let Some(byte) = remaining.next() else {
                    break;
                };
                chunk.push(byte);
            }
            if chunk.is_empty() {
                return None;
            }

            loaded += chunk.len() as u64;
            on_progress(UploadProgress { loaded, total });
            Some((
                Ok(chunk),
                (remaining, loaded, reported_initial, on_progress),
            ))
        },
    );
    reqwest::Body::wrap_stream(stream)
}
