//! A deliberately wasteful plugin, so there is something to watch.
//!
//! On a workstation there are no job cgroups and nothing running, so an agent
//! reports an idle process doing nothing — which proves the pipeline works and
//! shows nothing about it. `burn` gives it work to find: threads spinning on a duty
//! cycle, and a ballast allocation that grows in steps and is released, so CPU and
//! RSS both move in a shape you can recognise from the other end.
//!
//! Two things make it more than a toy. Its threads are named `burn/<worker>`, so
//! [`cs_plugin_selfmon`](../../../crates/plugins/selfmon) attributes their CPU to
//! this service by name and the head prints it — the `<service>/<worker>`
//! convention, visible end to end. And it is an ordinary plugin: it depends on
//! `cs-api` alone, exactly as a plugin in `crates/plugins/` does, so it is also a
//! worked example of the smallest one that does anything.
//!
//! It sends no metrics. `type Data = NoData` is how it says so: `sample` returns
//! `Vec<NoData>`, which cannot be non-empty. Everything it does shows up in
//! somebody else's numbers.

use std::hint::black_box;
use std::thread;
use std::time::{Duration, Instant};

use cs_api::{
    JobInfo, NoCommand, NoData, Result, Sampler, ServiceBound, ServiceCtx, ServiceDef,
    ShutdownSignal,
};
use tracing::{debug, info};

/// How long one duty cycle lasts.
///
/// Short enough that shutdown is not kept waiting — a worker notices the signal
/// between slices — and long enough that the spinning is not all scheduler
/// overhead.
const SLICE: Duration = Duration::from_millis(200);

/// Pages are 4 KiB on every platform this runs on. Being wrong only costs a few
/// redundant writes, not correctness: the point is to touch each page once.
const PAGE: usize = 4096;

/// How many steps the ballast grows in before it is released.
const BALLAST_STEPS: usize = 4;

/// The load-generating service.
pub struct BurnService;

impl ServiceDef for BurnService {
    const NAME: &'static str = "burn";
    /// Nothing. See the module docs.
    type Data = NoData;
    type Command = NoCommand;
}

/// How much of a nuisance to be.
#[derive(Clone, Debug)]
pub struct BurnConfig {
    /// How many threads spin.
    pub threads: usize,

    /// The fraction of each thread's time spent spinning, in `0.0..=1.0`.
    pub duty: f64,

    /// Peak bytes of ballast held. Reached in [`BALLAST_STEPS`] steps, then freed.
    pub ballast: usize,

    /// How often the ballast moves. Also this service's sampling interval, since
    /// the churn happens in `sample`.
    pub interval: Duration,
}

impl Default for BurnConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            duty: 0.25,
            ballast: 64 * 1024 * 1024,
            interval: Duration::from_secs(5),
        }
    }
}

/// Burns CPU in worker threads and moves memory around in `sample`.
pub struct Burn {
    config: BurnConfig,
    /// Held so it stays resident. Dropped all at once, which is the point.
    ballast: Vec<Vec<u8>>,
    step: u64,
}

impl Burn {
    /// A load generator that has not started yet.
    #[must_use]
    pub const fn new(config: BurnConfig) -> Self {
        Self {
            config,
            ballast: Vec::new(),
            step: 0,
        }
    }

    /// A factory the engine can rebuild from, for `Restart` and after a panic.
    pub fn factory(config: BurnConfig) -> impl FnMut() -> Self + Send + 'static {
        move || Self::new(config.clone())
    }

    /// Grow the ballast by one step, or release all of it if it is already full.
    fn churn(&mut self) {
        self.step += 1;

        if self.ballast.len() >= BALLAST_STEPS {
            self.ballast = Vec::new();
            debug!(step = self.step, "released the ballast");
            return;
        }

        let mut block = vec![0u8; self.config.ballast / BALLAST_STEPS];
        // Touching every page is the whole job. A fresh allocation this size comes
        // from `mmap`, and until something writes to a page the kernel has not
        // given it one — so an untouched `Vec` moves RSS not at all, and RSS is
        // what selfmon reports.
        #[expect(clippy::cast_possible_truncation, reason = "any byte will do")]
        let mark = self.step as u8;
        for page in block.chunks_mut(PAGE) {
            page[0] = mark;
        }
        self.ballast.push(block);
    }
}

impl ServiceBound for Burn {
    type Service = BurnService;
}

/// Built-ins only: `Shutdown` and `Restart` both work through the default reply,
/// so the engine stops or rebuilds this service without it having to say anything.
impl cs_api::CommandReceiver for Burn {}

impl Sampler for Burn {
    fn interval(&self) -> Duration {
        self.config.interval
    }

    fn start(&mut self, ctx: &ServiceCtx<BurnService>) -> Result<()> {
        // `start` runs again after every rebuild, and the engine has joined the old
        // instance's threads by then, so there is no risk of doubling up.
        for worker in 0..self.config.threads {
            let shutdown = ctx.shutdown_signal().clone();
            let duty = self.config.duty;
            ctx.spawn_blocking_worker(&format!("spin{worker}"), move || spin(&shutdown, duty))?;
        }
        info!(
            threads = self.config.threads,
            duty = self.config.duty,
            ballast = self.config.ballast,
            "burning"
        );
        Ok(())
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<NoData>> {
        // On this service's own thread, `burn/sample`, so the cost of touching the
        // pages lands under that name in the agent's own report.
        self.churn();
        Ok(Vec::new())
    }
}

/// Spin for `duty` of each slice, idle for the rest, until shutdown.
///
/// The one place in this repo where `Instant::now` and `thread::sleep` are right.
/// The rule against them is about engine and sampler-loop timing, which has to be
/// drivable by a test clock; this is a plugin's own blocking worker, and what it
/// measures is the wall-clock time it is occupying a core for — a paused clock
/// would make it burn nothing at all.
fn spin(shutdown: &ShutdownSignal, duty: f64) {
    let busy = SLICE.mul_f64(duty.clamp(0.0, 1.0));
    let idle = SLICE.saturating_sub(busy);

    // A cheap mixing step, written back through `black_box` so the optimiser
    // cannot notice that nothing reads the result and delete the loop.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    while !shutdown.is_set() {
        let until = Instant::now() + busy;
        while Instant::now() < until {
            for _ in 0..4096 {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
            }
            black_box(state);
        }
        if !idle.is_zero() {
            thread::sleep(idle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cs_api::test_support::FakeEngine;

    /// The ballast has to actually move, in both directions.
    #[test]
    fn the_ballast_grows_in_steps_and_is_released() {
        let mut burn = Burn::new(BurnConfig {
            ballast: 4 * PAGE * BALLAST_STEPS,
            ..BurnConfig::default()
        });

        let mut sizes = Vec::new();
        for _ in 0..=BALLAST_STEPS + 1 {
            burn.churn();
            sizes.push(burn.ballast.iter().map(Vec::len).sum::<usize>());
        }

        assert_eq!(
            sizes,
            vec![
                4 * PAGE,
                8 * PAGE,
                12 * PAGE,
                16 * PAGE,
                0,        // full, so released
                4 * PAGE, // and growing again
            ]
        );
    }

    /// Every page of a block is written to, or RSS would not move.
    #[test]
    fn every_page_of_the_ballast_is_touched() {
        let mut burn = Burn::new(BurnConfig {
            ballast: 3 * PAGE * BALLAST_STEPS,
            ..BurnConfig::default()
        });
        burn.churn();

        let block = burn.ballast.first().expect("one block");
        assert_eq!(block.len(), 3 * PAGE);
        for page in 0..3 {
            assert_eq!(block[page * PAGE], 1, "page {page} was not touched");
        }
    }

    /// Registration through the real `ServiceCtx`, without an engine: the worker
    /// threads are asked for by name and stop when the signal fires.
    #[test]
    fn start_asks_for_one_named_thread_per_burner() {
        let engine = FakeEngine::new("node-1");
        let ctx = engine.ctx::<BurnService>();

        let mut burn = Burn::new(BurnConfig {
            threads: 3,
            duty: 0.0, // no point spinning in a unit test
            ..BurnConfig::default()
        });
        burn.start(&ctx).expect("start");

        assert_eq!(
            engine.worker_names(),
            vec!["burn/spin0", "burn/spin1", "burn/spin2"]
        );
        // Under the 15-byte kernel limit, which is why the service name is short.
        for name in engine.worker_names() {
            assert!(name.len() <= cs_api::MAX_THREAD_NAME_LEN, "{name}");
        }

        // `FakeEngine` starts real threads, so end them the way the engine would.
        engine.shutdown_signal().set();
        engine.join_workers();
    }

    /// A `spin` that is asked to stop before it starts does not spin at all, which
    /// is what makes the worker safe to join under the shutdown deadline.
    #[test]
    fn a_burner_returns_when_shutdown_is_already_set() {
        let shutdown = ShutdownSignal::new();
        shutdown.set();
        let started = Instant::now();
        spin(&shutdown, 1.0);
        assert!(started.elapsed() < SLICE, "spun after being told to stop");
    }
}
