//! The system's microphones through cpal: the only lines no test can run, so the gate exempts this
//! file; everything around them is `ear.rs`, driven in the suite by its fake.

use crate::ear::{Inputs, Ready, Sound, Source};
use coreaudio::audio_unit::macos_helpers::{
    get_audio_device_ids, get_default_device_id, get_device_name, get_device_transport_type,
};
use cpal::traits::{DeviceTrait as _, HostTrait as _, StreamTrait as _};
use std::error::Error;
use std::sync::mpsc::Receiver;

/// The system's inputs, by the names cpal gives them.
pub struct Mic;

impl Source for Mic {
    fn hold(&self, name: &str, ready: Ready, mut sound: Sound, stop: Receiver<()>) {
        let opened = || -> Result<_, Box<dyn Error>> {
            let device = (cpal::default_host().input_devices()?)
                .find(|device| device.description().is_ok_and(|held| held.name() == name))
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
                ready(Ok((rate, channels)));
                stop.recv().unwrap_or_default();
                drop(stream);
            }
            Err(cause) => ready(Err(cause.to_string())),
        }
    }

    fn inputs(&self) -> Inputs {
        // names read fallibly: a device gone mid-listing is skipped, where its Display would panic
        let names: Vec<String> = (cpal::default_host().input_devices().into_iter().flatten())
            .filter_map(|device| Some(device.description().ok()?.name().to_owned()))
            .collect();
        // a device's `kAudioDevicePropertyTransportType`, the four letters of its constant
        let transport = |id| {
            get_device_transport_type(id)
                .unwrap_or_default()
                .to_be_bytes()
        };
        // the default read once: its name and its transport are one device's
        let default = get_default_device_id(true);
        Inputs {
            built: (get_audio_device_ids().into_iter().flatten())
                .filter(|&id| &transport(id) == b"bltn")
                .filter_map(|id| get_device_name(id).ok())
                .find(|name| names.contains(name)),
            default: default.and_then(|id| get_device_name(id).ok()),
            names,
            wireless: default.is_some_and(|id| matches!(&transport(id), b"blue" | b"blea")),
        }
    }
}
