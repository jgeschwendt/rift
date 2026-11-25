//! The system's microphones through cpal: the only lines no test can run, so the gate exempts this
//! file; everything around them is `ear.rs`, driven in the suite by its fake.

use crate::ear::{Ready, Sound, Source};
use cpal::traits::{DeviceTrait as _, HostTrait as _, StreamTrait as _};
use std::error::Error;
use std::sync::mpsc::Receiver;

/// The system's inputs, by the names cpal gives them.
pub struct Mic;

impl Source for Mic {
    fn hold(&self, name: &str, ready: Ready, mut sound: Sound, stop: Receiver<()>) {
        let opened = || -> Result<_, Box<dyn Error>> {
            let device = (cpal::default_host().input_devices()?)
                .find(|device| device.to_string() == name)
                .ok_or("not among the inputs")?;
            let config = device.default_input_config()?.config();
            let stream = device.build_input_stream(
                config,
                move |data: &[f32], _: &_| sound(data),
                drop,
                None,
            )?;
            stream.play()?;
            Ok((stream, config.sample_rate, config.channels))
        };
        match opened() {
            Ok((stream, rate, channels)) => {
                drop(ready.send(Ok((rate, channels))));
                stop.recv().unwrap_or_default();
                drop(stream);
            }
            Err(cause) => drop(ready.send(Err(cause.to_string()))),
        }
    }

    fn inputs(&self) -> (Option<String>, Vec<String>) {
        let host = cpal::default_host();
        let names = host.input_devices().into_iter().flatten();
        (
            host.default_input_device().map(|device| device.to_string()),
            names.map(|device| device.to_string()).collect(),
        )
    }
}
