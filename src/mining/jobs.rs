use crate::bitcoin::template::StratumJob;
use std::{collections::VecDeque, sync::Arc, time::Instant};

pub const JOB_HISTORY_DEPTH: usize = 16;

#[derive(Debug, Clone)]
pub struct JobEntry {
    pub job: Arc<StratumJob>,
    #[allow(dead_code)]
    pub created_at: Instant,
    pub superseded_by_clean: bool,
}

/// Bounded history of the exact payout-specific jobs issued to one session.
pub struct IssuedJobs {
    entries: VecDeque<JobEntry>,
}

impl IssuedJobs {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::with_capacity(JOB_HISTORY_DEPTH),
        }
    }

    pub fn issue(&mut self, job: Arc<StratumJob>, clean: bool) {
        if clean {
            for entry in &mut self.entries {
                entry.superseded_by_clean = true;
            }
        }
        if self.entries.len() >= JOB_HISTORY_DEPTH {
            self.entries.pop_front();
        }
        self.entries.push_back(JobEntry {
            job,
            created_at: Instant::now(),
            superseded_by_clean: false,
        });
    }

    pub fn find(&self, job_id: &str) -> Option<JobEntry> {
        self.entries
            .iter()
            .find(|entry| entry.job.job_id == job_id)
            .cloned()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl Default for IssuedJobs {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: usize) -> Arc<StratumJob> {
        Arc::new(StratumJob {
            job_id: id.to_string(),
            prev_hash: String::new(),
            coinbase1: Vec::new(),
            coinbase2: Vec::new(),
            merkle_branch: Vec::new(),
            merkle_branch_raw: Vec::new(),
            version: 0,
            bits: String::new(),
            cur_time: 0,
            height: 0,
            network_target: [0; 32],
            coinbase_template: Vec::new(),
            extranonce_offset: 0,
            extranonce1_len: 0,
            extranonce2_len: 0,
            transactions: Arc::new(Vec::new()),
            payout_address: "test".to_string(),
            has_witness_commitment: false,
        })
    }

    #[test]
    fn history_depth_is_bounded() {
        let mut history = IssuedJobs::new();
        for id in 0..JOB_HISTORY_DEPTH + 2 {
            history.issue(job(id), false);
        }
        assert_eq!(history.entries.len(), JOB_HISTORY_DEPTH);
        assert!(history.find("0").is_none());
        assert!(history.find(&(JOB_HISTORY_DEPTH + 1).to_string()).is_some());
    }

    #[test]
    fn clean_job_supersedes_all_previous_jobs() {
        let mut history = IssuedJobs::new();
        history.issue(job(1), false);
        history.issue(job(2), false);
        history.issue(job(3), true);

        assert!(history.find("1").unwrap().superseded_by_clean);
        assert!(history.find("2").unwrap().superseded_by_clean);
        assert!(!history.find("3").unwrap().superseded_by_clean);
    }
}
