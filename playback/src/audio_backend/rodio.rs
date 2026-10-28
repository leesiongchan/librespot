use std::process::exit;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

use cpal::traits::{DeviceTrait, HostTrait};
use thiserror::Error;

use super::{Sink, SinkError, SinkResult};
use crate::config::AudioFormat;
use crate::convert::Converter;
use crate::decoder::AudioPacket;
use crate::{NUM_CHANNELS, SAMPLE_RATE};

#[cfg(all(
    feature = "rodiojack-backend",
    not(any(target_os = "linux", target_os = "dragonfly", target_os = "freebsd"))
))]
compile_error!("Rodio JACK backend is currently only supported on linux.");

#[cfg(feature = "rodio-backend")]
pub fn mk_rodio(device: Option<String>, format: AudioFormat) -> Box<dyn Sink> {
    Box::new(open(cpal::default_host(), device, format))
}

#[cfg(feature = "rodio-backend")]
pub fn mk_rodio_with_error_signal(
    device: Option<String>,
    format: AudioFormat,
    failed: Arc<AtomicBool>,
) -> Box<dyn Sink> {
    Box::new(open_with_error_signal(
        cpal::default_host(),
        device,
        format,
        failed,
    ))
}

#[cfg(feature = "rodiojack-backend")]
pub fn mk_rodiojack(device: Option<String>, format: AudioFormat) -> Box<dyn Sink> {
    Box::new(open(
        cpal::host_from_id(cpal::HostId::Jack).unwrap(),
        device,
        format,
    ))
}

#[derive(Debug, Error)]
pub enum RodioError {
    #[error("<RodioSink> No Device Available")]
    NoDeviceAvailable,
    #[error("<RodioSink> device \"{0}\" is Not Available")]
    DeviceNotAvailable(String),
    #[error("<RodioSink> Play Error: {0}")]
    PlayError(#[from] rodio::PlayError),
    #[error("<RodioSink> Stream Error: {0}")]
    StreamError(#[from] rodio::StreamError),
    #[error("<RodioSink> Cannot Get Audio Devices: {0}")]
    DevicesError(#[from] cpal::DevicesError),
    #[error("<RodioSink> {0}")]
    Samples(String),
}

impl From<RodioError> for SinkError {
    fn from(e: RodioError) -> SinkError {
        use RodioError::*;
        let es = e.to_string();
        match e {
            StreamError(_) | PlayError(_) | Samples(_) => SinkError::OnWrite(es),
            NoDeviceAvailable | DeviceNotAvailable(_) => SinkError::ConnectionRefused(es),
            DevicesError(_) => SinkError::InvalidParams(es),
        }
    }
}

impl From<cpal::DefaultStreamConfigError> for RodioError {
    fn from(_: cpal::DefaultStreamConfigError) -> RodioError {
        RodioError::NoDeviceAvailable
    }
}

impl From<cpal::SupportedStreamConfigsError> for RodioError {
    fn from(_: cpal::SupportedStreamConfigsError) -> RodioError {
        RodioError::NoDeviceAvailable
    }
}

pub struct RodioSink {
    rodio_sink: rodio::Sink,
    _stream: rodio::OutputStream,
    shutdown: Option<Arc<AtomicBool>>,
    failed: Arc<AtomicBool>,
}

fn list_formats(device: &cpal::Device) {
    match device.default_output_config() {
        Ok(cfg) => {
            debug!("  Default config:");
            debug!("    {cfg:?}");
        }
        Err(e) => {
            // Use loglevel debug, since even the output is only debug
            debug!("Error getting default rodio::Sink config: {e}");
        }
    };

    match device.supported_output_configs() {
        Ok(mut cfgs) => {
            if let Some(first) = cfgs.next() {
                debug!("  Available configs:");
                debug!("    {first:?}");
            } else {
                return;
            }

            for cfg in cfgs {
                debug!("    {cfg:?}");
            }
        }
        Err(e) => {
            debug!("Error getting supported rodio::Sink configs: {e}");
        }
    }
}

fn list_outputs(host: &cpal::Host) -> Result<(), cpal::DevicesError> {
    let mut default_device_name = None;

    if let Some(default_device) = host.default_output_device() {
        default_device_name = default_device.name().ok();
        println!(
            "Default Audio Device:\n  {}",
            default_device_name.as_deref().unwrap_or("[unknown name]")
        );

        list_formats(&default_device);

        println!("Other Available Audio Devices:");
    } else {
        warn!("No default device was found");
    }

    for device in host.output_devices()? {
        match device.name() {
            Ok(name) if Some(&name) == default_device_name.as_ref() => (),
            Ok(name) => {
                println!("  {name}");
                list_formats(&device);
            }
            Err(e) => {
                warn!("Cannot get device name: {e}");
                println!("   [unknown name]");
                list_formats(&device);
            }
        }
    }

    Ok(())
}

fn create_sink(
    host: &cpal::Host,
    device: Option<String>,
    format: AudioFormat,
    failed: Arc<AtomicBool>,
) -> Result<(rodio::Sink, rodio::OutputStream), RodioError> {
    let cpal_device = match device.as_deref() {
        Some("?") => match list_outputs(host) {
            Ok(()) => exit(0),
            Err(e) => {
                error!("{e}");
                exit(1);
            }
        },
        Some(device_name) => {
            // Ignore devices for which getting name fails, or format doesn't match
            host.output_devices()?
                .find(|d| d.name().ok().is_some_and(|name| name == device_name)) // Ignore devices for which getting name fails
                .ok_or_else(|| RodioError::DeviceNotAvailable(device_name.to_string()))?
        }
        None => host
            .default_output_device()
            .ok_or(RodioError::NoDeviceAvailable)?,
    };

    let name = cpal_device.name().ok();
    info!(
        "Using audio device: {}",
        name.as_deref().unwrap_or("[unknown name]")
    );

    // First try native stereo 44.1 kHz playback, then fall back to the device default sample rate
    // (some devices only support 48 kHz and Rodio will resample linearly), then fall back to
    // whatever the default device config is (like mono).
    let default_config = cpal_device.default_output_config()?;
    let config = cpal_device
        .supported_output_configs()?
        .find(|c| c.channels() == NUM_CHANNELS as cpal::ChannelCount)
        .and_then(|c| {
            c.try_with_sample_rate(cpal::SampleRate(SAMPLE_RATE))
                .or_else(|| c.try_with_sample_rate(default_config.sample_rate()))
        })
        .unwrap_or(default_config);

    let sample_format = match format {
        AudioFormat::F64 => cpal::SampleFormat::F64,
        AudioFormat::F32 => cpal::SampleFormat::F32,
        AudioFormat::S32 => cpal::SampleFormat::I32,
        AudioFormat::S24 | AudioFormat::S24_3 => cpal::SampleFormat::I24,
        AudioFormat::S16 => cpal::SampleFormat::I16,
    };

    let mut stream = match rodio::OutputStreamBuilder::default()
        .with_device(cpal_device.clone())
        .with_config(&config.config())
        .with_sample_format(sample_format)
        .with_error_callback(output_error_callback(failed.clone()))
        .open_stream()
    {
        Ok(exact_stream) => exact_stream,
        Err(e) => {
            warn!("unable to create Rodio output, falling back to default: {e}");
            rodio::OutputStreamBuilder::from_device(cpal_device)?
                .with_error_callback(output_error_callback(failed))
                .open_stream_or_fallback()?
        }
    };

    // disable logging on stream drop
    stream.log_on_drop(false);

    let sink = rodio::Sink::connect_new(stream.mixer());
    Ok((sink, stream))
}

pub fn open(host: cpal::Host, device: Option<String>, format: AudioFormat) -> RodioSink {
    open_with_error_signal(host, device, format, Arc::new(AtomicBool::new(false)))
}

fn open_with_error_signal(
    host: cpal::Host,
    device: Option<String>,
    format: AudioFormat,
    failed: Arc<AtomicBool>,
) -> RodioSink {
    info!(
        "Using Rodio sink with format {format:?} and cpal host: {}",
        host.id().name()
    );

    let (sink, stream) = create_sink(&host, device, format, failed.clone()).unwrap();

    debug!("Rodio sink was created");
    RodioSink {
        rodio_sink: sink,
        _stream: stream,
        shutdown: None,
        failed,
    }
}

impl Sink for RodioSink {
    fn set_shutdown_signal(&mut self, shutdown: Arc<AtomicBool>) {
        self.shutdown = Some(shutdown);
    }
    fn start(&mut self) -> SinkResult<()> {
        if self.failed.load(Ordering::Acquire) {
            // A start error pauses inside a Playing-only poll branch and can abort the process.
            self.rodio_sink.stop();
        } else {
            self.rodio_sink.play();
        }
        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        if !wait_for_output(
            || !self.rodio_sink.empty(),
            self.shutdown.as_deref(),
            &self.failed,
        ) {
            self.rodio_sink.stop();
        }
        self.rodio_sink.pause();
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, converter: &mut Converter) -> SinkResult<()> {
        if self
            .shutdown
            .as_ref()
            .is_some_and(|shutdown| shutdown.load(Ordering::Acquire))
        {
            self.rodio_sink.stop();
            return Ok(());
        }
        check_output(&self.rodio_sink, &self.failed, self.shutdown.as_deref())?;
        let samples = packet
            .samples()
            .map_err(|e| RodioError::Samples(e.to_string()))?;
        let samples_f32: &[f32] = &converter.f64_to_f32(samples);
        let source = rodio::buffer::SamplesBuffer::new(
            NUM_CHANNELS as cpal::ChannelCount,
            SAMPLE_RATE,
            samples_f32,
        );
        self.rodio_sink.append(source);

        // Chunk sizes seem to be about 256 to 3000 ish items long.
        // Assuming they're on average 1628 then a half second buffer is:
        // 44100 elements --> about 27 chunks
        if !wait_for_output(
            || self.rodio_sink.len() > 26,
            self.shutdown.as_deref(),
            &self.failed,
        ) {
            self.rodio_sink.stop();
        }
        check_output(&self.rodio_sink, &self.failed, self.shutdown.as_deref())
    }
}

fn check_output(
    sink: &rodio::Sink,
    failed: &AtomicBool,
    shutdown: Option<&AtomicBool>,
) -> SinkResult<()> {
    if failed.load(Ordering::Acquire)
        && !shutdown.is_some_and(|shutdown| shutdown.load(Ordering::Acquire))
    {
        sink.stop();
        return Err(SinkError::NotConnected("audio output failed".to_string()));
    }
    Ok(())
}

fn output_error_callback(failed: Arc<AtomicBool>) -> impl FnMut(cpal::StreamError) + Clone {
    // No logging, allocation, or engine callbacks on the audio thread.
    move |_error| failed.store(true, Ordering::Release)
}

fn wait_for_output(
    mut pending: impl FnMut() -> bool,
    shutdown: Option<&AtomicBool>,
    failed: &AtomicBool,
) -> bool {
    while pending() {
        if failed.load(Ordering::Acquire)
            || shutdown.is_some_and(|shutdown| shutdown.load(Ordering::Acquire))
        {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
    true
}

impl RodioSink {
    #[allow(dead_code)]
    pub const NAME: &'static str = "rodio";
}

#[cfg(test)]
mod teardown_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    fn stalled_wait(
        buffered: usize,
        limit: usize,
    ) -> (
        Arc<AtomicUsize>,
        Arc<AtomicBool>,
        mpsc::Receiver<bool>,
        thread::JoinHandle<()>,
    ) {
        let queued = Arc::new(AtomicUsize::new(buffered));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let pending = queued.clone();
        let cancelled = shutdown.clone();
        let thread = thread::spawn(move || {
            let mut started = Some(started_tx);
            let drained = wait_for_output(
                || {
                    if let Some(started) = started.take() {
                        started.send(()).unwrap();
                    }
                    pending.load(Ordering::Acquire) > limit
                },
                Some(&cancelled),
                &AtomicBool::new(false),
            );
            done_tx.send(drained).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        (queued, shutdown, done_rx, thread)
    }

    #[test]
    fn teardown_cancels_a_stalled_drain() {
        let (queued, shutdown, done, thread) = stalled_wait(1, 0);
        shutdown.store(true, Ordering::Release);
        let result = done.recv_timeout(Duration::from_secs(1));
        queued.store(0, Ordering::Release);
        thread.join().unwrap();
        assert_eq!(result, Ok(false));
    }

    #[test]
    fn teardown_cancels_stalled_backpressure() {
        let (queued, shutdown, done, thread) = stalled_wait(27, 26);
        shutdown.store(true, Ordering::Release);
        let result = done.recv_timeout(Duration::from_secs(1));
        queued.store(0, Ordering::Release);
        thread.join().unwrap();
        assert_eq!(result, Ok(false));
    }

    #[test]
    fn normal_stop_waits_for_output_to_drain() {
        let (queued, _shutdown, done, thread) = stalled_wait(1, 0);
        assert!(matches!(done.try_recv(), Err(mpsc::TryRecvError::Empty)));
        queued.store(0, Ordering::Release);
        assert_eq!(done.recv_timeout(Duration::from_secs(1)), Ok(true));
        thread.join().unwrap();
    }

    #[test]
    fn output_error_callback_latches_repeated_faults() {
        let failed = Arc::new(AtomicBool::new(false));
        let mut callback = output_error_callback(failed.clone());
        callback(cpal::StreamError::DeviceNotAvailable);
        callback(cpal::StreamError::DeviceNotAvailable);
        assert!(failed.load(Ordering::Acquire));
    }

    #[test]
    fn output_fault_cancels_stalled_drain_and_backpressure() {
        for limit in [0, 26] {
            let failed = Arc::new(AtomicBool::new(false));
            let fault = failed.clone();
            let queued = Arc::new(AtomicUsize::new(limit + 1));
            let pending = queued.clone();
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let worker = thread::spawn(move || {
                let mut started = Some(started_tx);
                let drained = wait_for_output(
                    || {
                        if let Some(started) = started.take() {
                            started.send(()).unwrap();
                        }
                        pending.load(Ordering::Acquire) > limit
                    },
                    None,
                    &fault,
                );
                done_tx.send(drained).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            output_error_callback(failed)(cpal::StreamError::DeviceNotAvailable);
            let result = done_rx.recv_timeout(Duration::from_secs(1));
            queued.store(0, Ordering::Release);
            worker.join().unwrap();
            assert_eq!(result, Ok(false));
        }
    }

    #[test]
    fn faulted_write_reports_failure_but_intentional_shutdown_stays_quiet() {
        let (mixer, _source) = rodio::mixer::mixer(NUM_CHANNELS as cpal::ChannelCount, SAMPLE_RATE);
        let sink = rodio::Sink::connect_new(&mixer);
        let failed = AtomicBool::new(false);
        let shutdown = AtomicBool::new(false);
        assert!(check_output(&sink, &failed, Some(&shutdown)).is_ok());
        failed.store(true, Ordering::Release);
        assert!(matches!(
            check_output(&sink, &failed, Some(&shutdown)),
            Err(SinkError::NotConnected(_))
        ));
        shutdown.store(true, Ordering::Release);
        assert!(check_output(&sink, &failed, Some(&shutdown)).is_ok());
    }

    #[test]
    #[ignore = "requires a usable system audio output device"]
    fn faulted_sink_keeps_start_and_stop_safe_and_rejects_writes() {
        let failed = Arc::new(AtomicBool::new(false));
        let mut sink = open_with_error_signal(
            cpal::default_host(),
            None,
            AudioFormat::default(),
            failed.clone(),
        );
        output_error_callback(failed)(cpal::StreamError::DeviceNotAvailable);
        assert!(sink.start().is_ok());
        let mut converter = Converter::new(None);
        assert!(matches!(
            sink.write(AudioPacket::Samples(vec![0.0; 2]), &mut converter),
            Err(SinkError::NotConnected(_))
        ));
        assert!(sink.stop().is_ok());
        sink.set_shutdown_signal(Arc::new(AtomicBool::new(true)));
        assert!(
            sink.write(AudioPacket::Samples(vec![0.0; 2]), &mut converter)
                .is_ok()
        );
        assert!(sink.stop().is_ok());
    }
}
