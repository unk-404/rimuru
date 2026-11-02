//! Byte-counted, conservative Reno-style window, pacing and Karn RTT samples.
use std::time::{Duration, Instant};
pub(super) struct Recovery {
    pub rto: Duration,
    pub srtt: Option<Duration>,
    variance: Duration,
    pub inflight: usize,
    cwnd: usize,
    threshold: usize,
    d: usize,
    loopback: bool,
    next: Instant,
    last_loss: Instant,
}
impl Recovery {
    pub fn new(d: usize, rto: Duration, loopback: bool) -> Self {
        let now = Instant::now();
        Self {
            rto,
            srtt: None,
            variance: Duration::ZERO,
            inflight: 0,
            cwnd: 10 * d,
            threshold: usize::MAX,
            d,
            loopback,
            next: now,
            last_loss: now - rto,
        }
    }
    pub fn can_send(&self, bytes: usize, now: Instant) -> bool {
        self.loopback || (self.inflight.saturating_add(bytes) <= self.cwnd && now >= self.next)
    }
    pub fn sent(&mut self, bytes: usize, now: Instant) {
        self.inflight += bytes;
        if !self.loopback {
            let rtt = self.srtt.unwrap_or(Duration::from_millis(100));
            self.next = now + rtt.mul_f64(bytes as f64 / self.cwnd as f64);
        }
    }
    pub fn ack(&mut self, bytes: usize, rtt: Option<Duration>) {
        self.inflight = self.inflight.saturating_sub(bytes);
        self.cwnd = self
            .cwnd
            .saturating_add(if self.cwnd < self.threshold {
                bytes
            } else {
                (self.d * bytes / self.cwnd).max(1)
            })
            .min(16 << 20);
        if let Some(rtt) = rtt {
            if let Some(old) = self.srtt {
                self.variance = self.variance.mul_f64(0.75) + old.abs_diff(rtt).mul_f64(0.25);
                self.srtt = Some(old.mul_f64(0.875) + rtt.mul_f64(0.125));
            } else {
                self.srtt = Some(rtt);
                self.variance = rtt / 2;
            }
            let minimum = if self.loopback { 4 } else { 1 };
            self.rto = (self.srtt.unwrap() + self.variance * 4)
                .clamp(Duration::from_millis(minimum), Duration::from_secs(60));
        }
    }
    pub fn lost(&mut self, bytes: usize, now: Instant) {
        self.inflight = self.inflight.saturating_sub(bytes);
        if now.duration_since(self.last_loss) >= self.srtt.unwrap_or(self.rto) {
            self.threshold = (self.cwnd / 2).max(self.d * 2);
            self.cwnd = self.threshold;
            self.last_loss = now;
        }
    }
    pub fn rack_delay(&self) -> Duration {
        (self.srtt.unwrap_or(self.rto) / 4).max(Duration::from_millis(1))
    }
}
