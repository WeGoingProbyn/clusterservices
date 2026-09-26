use std::time::Duration;

use cs_util::Result;

use crate::{CommandReceiver, Data, JobInfo, ServiceCtx};

/// A service that produces data on a timer — the shape almost every agent-side
/// plugin takes.
///
/// A convenience adapter, not the only option: the engine gives each sampler one
/// blocking thread running a sleep / sample / check-for-commands loop, and calls
/// back into these methods from it. A plugin that needs more than one thread uses
/// [`start`](Sampler::start) to reach [`ServiceCtx`] and spawn its own.
///
/// Samplers are registered as **factories** (`.sampler(|| Cpu::new())`) so the
/// engine can rebuild one after a `Restart` command or a panic.
///
/// Everything here runs on the sampler's own thread, one call at a time, so
/// `&mut self` is free — no locks, no `Sync`. Blocking file reads are expected.
///
/// ```
/// use std::time::Duration;
/// use cs_api::{CommandReceiver, JobInfo, NoCommand, Sampler, ServiceBound, ServiceDef};
/// use cs_util::Result;
///
/// struct Procs;
/// impl ServiceDef for Procs {
///     const NAME: &'static str = "procs";
///     type Data = String; // stands in for a generated prost message
///     type Command = NoCommand;
/// }
///
/// struct ProcSampler;
///
/// impl ServiceBound for ProcSampler {
///     type Service = Procs;
/// }
/// impl CommandReceiver for ProcSampler {} // built-ins only
///
/// impl Sampler for ProcSampler {
///     fn interval(&self) -> Duration {
///         Duration::from_secs(5)
///     }
///
///     fn sample(&mut self, jobs: &[JobInfo]) -> Result<Vec<String>> {
///         Ok(jobs.iter().map(|job| format!("{job}")).collect())
///     }
/// }
/// ```
pub trait Sampler: CommandReceiver + Send + 'static {
    /// How long to wait between samples.
    ///
    /// Read before every sleep, so a sampler can retune itself in response to a
    /// custom command. Sample every few seconds; batching for the 30–60s send
    /// window is the sampler's own business.
    fn interval(&self) -> Duration;

    /// Take one sample of the jobs currently on the node.
    ///
    /// `jobs` is discovered once per tick by the engine and shared by every
    /// sampler, so all plugins see the same job list.
    ///
    /// Return **cumulative counters**, not rates — the server computes rates and
    /// can then survive a missed sample. A job whose cgroup vanished mid-sample
    /// is normal: skip it and return what the others produced. Reserve `Err` for
    /// "this sampler could not do its job at all"; the engine logs the whole
    /// error chain, counts it in
    /// [`ServiceStats::sample_errors`](crate::ServiceStats::sample_errors), and
    /// keeps the sampler running.
    fn sample(&mut self, jobs: &[JobInfo]) -> Result<Vec<Data<Self>>>;

    /// Called once on the sampler's thread before the first sample, and again
    /// after every rebuild.
    ///
    /// The hook for anything needing the engine: cloning the [`Outbox`](crate::Outbox)
    /// for extra workers, reading [`engine_stats`](ServiceCtx::engine_stats),
    /// taking a [`ShutdownSignal`](cs_async_util::ShutdownSignal). Returning an
    /// error fails the service's startup — the right response to "NVML is not
    /// present on this node".
    fn start(&mut self, _ctx: &ServiceCtx<Self::Service>) -> Result<()> {
        Ok(())
    }

    /// One last chance to emit data, during shutdown.
    ///
    /// Runs under the shutdown deadline, after the
    /// [`ShutdownSignal`](cs_async_util::ShutdownSignal) fires and before the
    /// writer drains. Flush partial batches here; do not start new work.
    fn on_shutdown(&mut self, _jobs: &[JobInfo]) -> Result<Vec<Data<Self>>> {
        Ok(Vec::new())
    }
}

/// Builds a sampler on demand.
///
/// Registration takes one of these rather than an instance so the engine can
/// rebuild the sampler after a `Restart` command, or after it panicked. Any
/// `FnMut() -> S` qualifies, which in practice means a closure like
/// `|| Cpu::new()`.
///
/// Fallible construction belongs in [`Sampler::start`], not here: a factory that
/// cannot fail keeps panic-recovery simple.
pub trait SamplerFactory: Send + 'static {
    /// The sampler this builds.
    type Sampler: Sampler;

    /// Build one.
    fn build(&mut self) -> Self::Sampler;
}

impl<S: Sampler, F: FnMut() -> S + Send + 'static> SamplerFactory for F {
    type Sampler = S;

    fn build(&mut self) -> S {
        self()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, NoCommand, Reply, ServiceBound, ServiceDef, StepId};

    struct Counter;

    impl ServiceDef for Counter {
        const NAME: &'static str = "counter";
        type Data = u64;
        type Command = u64;
    }

    /// Emits a running count, and lets an operator retune the interval.
    struct CountSampler {
        next: u64,
        interval: Duration,
        flushed: bool,
    }

    impl CountSampler {
        fn new() -> Self {
            Self {
                next: 0,
                interval: Duration::from_secs(1),
                flushed: false,
            }
        }
    }

    impl ServiceBound for CountSampler {
        type Service = Counter;
    }

    impl CommandReceiver for CountSampler {
        fn on_command(&mut self, cmd: Command<u64>) -> Reply {
            match cmd {
                Command::Custom(millis) => {
                    self.interval = Duration::from_millis(millis);
                    Reply::Handled
                }
                _ => Reply::Default,
            }
        }
    }

    impl Sampler for CountSampler {
        fn interval(&self) -> Duration {
            self.interval
        }

        fn sample(&mut self, jobs: &[JobInfo]) -> Result<Vec<u64>> {
            let start = self.next;
            self.next += jobs.len() as u64;
            Ok((start..self.next).collect())
        }

        fn on_shutdown(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
            self.flushed = true;
            Ok(vec![u64::MAX])
        }
    }

    fn jobs(n: u32) -> Vec<JobInfo> {
        (0..n)
            .map(|i| JobInfo::new(i, StepId::Batch, 1000, format!("/sys/fs/cgroup/job_{i}")))
            .collect()
    }

    #[test]
    fn a_sampler_sees_the_engines_job_list() {
        let mut sampler = CountSampler::new();
        assert_eq!(sampler.sample(&jobs(3)).unwrap(), [0, 1, 2]);
        assert_eq!(sampler.sample(&jobs(2)).unwrap(), [3, 4]);
        assert_eq!(sampler.sample(&[]).unwrap(), Vec::<u64>::new());
    }

    #[test]
    fn a_custom_command_can_retune_the_interval() {
        let mut sampler = CountSampler::new();
        assert_eq!(sampler.interval(), Duration::from_secs(1));
        assert_eq!(sampler.on_command(Command::Custom(250)), Reply::Handled);
        assert_eq!(sampler.interval(), Duration::from_millis(250));
        // Built-ins still fall through to the engine.
        assert_eq!(sampler.on_command(Command::Restart), Reply::Default);
    }

    #[test]
    fn on_shutdown_gets_a_final_flush() {
        let mut sampler = CountSampler::new();
        assert_eq!(sampler.on_shutdown(&jobs(1)).unwrap(), [u64::MAX]);
        assert!(sampler.flushed);
    }

    #[test]
    fn the_default_start_and_on_shutdown_do_nothing() {
        struct Bare;
        impl ServiceBound for Bare {
            type Service = Counter;
        }
        impl CommandReceiver for Bare {}
        impl Sampler for Bare {
            fn interval(&self) -> Duration {
                Duration::from_secs(1)
            }
            fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
                Ok(vec![1])
            }
        }

        let mut bare = Bare;
        assert!(bare.on_shutdown(&jobs(2)).unwrap().is_empty());
    }

    #[test]
    fn closures_are_sampler_factories() {
        let mut factory = CountSampler::new;
        let mut first = factory.build();
        assert_eq!(first.sample(&jobs(1)).unwrap(), [0]);

        // A rebuild starts from scratch, which is the point of registering a
        // factory rather than an instance.
        let mut second = factory.build();
        assert_eq!(second.sample(&jobs(1)).unwrap(), [0]);

        fn takes_factory<F: SamplerFactory>(_: F) {}
        takes_factory(CountSampler::new);
    }

    #[test]
    fn a_service_with_no_commands_needs_no_command_code() {
        struct Quiet;
        impl ServiceDef for Quiet {
            const NAME: &'static str = "quiet";
            type Data = u64;
            type Command = NoCommand;
        }
        struct QuietSampler;
        impl ServiceBound for QuietSampler {
            type Service = Quiet;
        }
        impl CommandReceiver for QuietSampler {}
        impl Sampler for QuietSampler {
            fn interval(&self) -> Duration {
                Duration::from_secs(30)
            }
            fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
                Ok(Vec::new())
            }
        }

        let mut quiet = QuietSampler;
        assert_eq!(quiet.on_command(Command::Shutdown), Reply::Default);
    }
}
