//! The system's outputs through cpal: the only lines no test can run, so the gate exempts this
//! file; everything around them is `mouth.rs`, driven in the suite by its fake.

use crate::mouth::{Fill, Outputs, Ready, Sink};
use cpal::traits::{DeviceTrait as _, HostTrait as _, StreamTrait as _};
use std::error::Error;
use std::io::{self, Write as _};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

/// The system's outputs, by the names cpal gives them.
pub struct Speaker;

impl Sink for Speaker {
    fn hold(&self, name: &str, ready: Ready, mut fill: Fill, stop: Receiver<()>) {
        // a stream cpal paused (its device gone, its configuration invalidated by a rate change
        // or a Bluetooth profile's) ends the hold; a glitch or a refused real-time promotion
        // plays on, a glitch (an I/O cycle the output missed, heard as a click) said on stderr
        // the 1st, 2nd, 4th… time an open has one, so a crackle shows in mouth.err unflooded
        let (failing, failed) = mpsc::channel();
        let (called, mut glitches) = (name.to_owned(), 0u64);
        let opened = || -> Result<_, Box<dyn Error>> {
            let device = (cpal::default_host().output_devices()?)
                .find(|device| device.description().is_ok_and(|held| held.name() == name))
                .ok_or("not among the outputs")?;
            let config = device.default_output_config()?.config();
            let (rate, channels) = (config.sample_rate, config.channels);
            // the latency cpal reports: the I/O buffer, CoreAudio's device latency and safety offset
            let stream = device.build_output_stream(
                config,
                move |data: &mut [f32], info: &cpal::OutputCallbackInfo| {
                    let stamp = info.timestamp();
                    let latency = stamp.playback.duration_since(stamp.callback).as_micros();
                    fill(
                        data,
                        rate,
                        channels,
                        u64::try_from(latency).unwrap_or_default(),
                    );
                },
                move |error: cpal::Error| {
                    if matches!(error.kind(), cpal::ErrorKind::Xrun) {
                        glitches = glitches.saturating_add(1);
                        if glitches.is_power_of_two() {
                            let line = format!("rift device: {called:?} glitched ({glitches})\n");
                            drop(io::stderr().write_all(line.as_bytes()));
                        }
                    }
                    if !matches!(
                        error.kind(),
                        cpal::ErrorKind::Xrun | cpal::ErrorKind::RealtimeDenied
                    ) {
                        failing.send(()).unwrap_or_default();
                    }
                },
                None,
            )?;
            stream.play()?;
            Ok((stream, rate))
        };
        match opened() {
            Ok((stream, rate)) => {
                ready(Ok(rate));
                while matches!(
                    stop.recv_timeout(Duration::from_millis(100)),
                    Err(RecvTimeoutError::Timeout)
                ) && failed.try_recv().is_err()
                {}
                drop(stream);
            }
            Err(cause) => ready(Err(cause.to_string())),
        }
    }

    fn outputs(&self) -> Outputs {
        let host = cpal::default_host();
        // names read fallibly: a device gone mid-listing is skipped, where its Display would panic
        let name = |device: cpal::Device| Some(device.description().ok()?.name().to_owned());
        Outputs {
            default: host.default_output_device().and_then(name),
            names: (host.output_devices().into_iter().flatten())
                .filter_map(name)
                .collect(),
        }
    }
}
