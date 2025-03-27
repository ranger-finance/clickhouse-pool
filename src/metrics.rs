use actix_web_prom::{PrometheusMetrics, PrometheusMetricsBuilder};
use prometheus::{CounterVec, GaugeVec, IntCounter, IntCounterVec, IntGaugeVec, Opts};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Prometheus(#[from] prometheus::Error),
}

#[derive(Debug, Default, Clone)]
pub enum Kind {
    #[default]
    Default,
    GaugeVec,
    CounterVec,
    IntGaugeVec,
    IntCounterVec,
    IntCounter,
}

#[derive(Debug, Default, Clone)]
pub struct MetricConfig<'a> {
    pub kind: Kind,
    pub name: &'a str,
    pub help: &'a str,
    pub label_names: &'a [&'a str],
}

pub type SharedRegistrar = Arc<Registrar>;

/// An abstracted metrics registrar for Prometheus.
#[derive(Clone)]
pub struct Registrar {
    prometheus: Arc<PrometheusMetrics>,
    int_counters_vecs: Arc<RwLock<HashMap<String, IntCounterVec>>>,
    int_counters: Arc<RwLock<HashMap<String, IntCounter>>>,
    int_gauges_vecs: Arc<RwLock<HashMap<String, IntGaugeVec>>>,
    counters_vecs: Arc<RwLock<HashMap<String, CounterVec>>>,
    gauges_vecs: Arc<RwLock<HashMap<String, GaugeVec>>>,
}

impl std::fmt::Debug for Registrar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registrar")
            .field("prometheus", &format!("{:?}", self.prometheus.registry))
            .field(
                "int_counters_vecs",
                &format!("{:?}", self.int_counters_vecs),
            )
            .field("int_gauges_vecs", &format!("{:?}", self.int_gauges_vecs))
            .field("counters_vecs", &format!("{:?}", self.counters_vecs))
            .field("gauges_vecs", &format!("{:?}", self.gauges_vecs))
            .finish()
    }
}

impl Default for Registrar {
    fn default() -> Self {
        Self {
            prometheus: Arc::new(PrometheusMetricsBuilder::new("default").build().unwrap()),
            int_counters_vecs: Default::default(),
            int_counters: Default::default(),
            int_gauges_vecs: Default::default(),
            counters_vecs: Default::default(),
            gauges_vecs: Default::default(),
        }
    }
}

pub trait Registry {
    fn with_metric_configs<'a>(&self, metrics: &'a [MetricConfig<'a>]) -> Result<(), Error>;
    fn with_metric_config<'a>(&self, metric: &'a MetricConfig<'a>) -> Result<(), Error>;
}

impl Registry for Registrar {
    fn with_metric_config<'a>(&self, metric: &'a MetricConfig<'a>) -> Result<(), Error> {
        log::info!(
            "Attempting to register metric with name {:?} and labels {:?}",
            metric.name,
            metric.label_names
        );
        match &metric.kind {
            Kind::Default => panic!("Registrar metric kind is `default`, so panicking."),
            Kind::GaugeVec => {
                let gauge =
                    GaugeVec::new(Opts::new(metric.name, metric.help), metric.label_names).unwrap();
                match self.prometheus.registry.register(Box::new(gauge.clone())) {
                    Ok(()) => {
                        self.gauges_vecs
                            .write()
                            .unwrap()
                            .insert(metric.name.to_string(), gauge);
                        Ok(())
                    }
                    Err(e) => {
                        if let prometheus::Error::AlreadyReg = e {
                            log::info!("Metric {:?} is already registered.", metric.name);
                            Ok(())
                        } else {
                            log::error!("Failed to register metric {:?}. {:?}", metric.name, e);
                            Err(Error::Prometheus(e))
                        }
                    }
                }
            }
            Kind::CounterVec => {
                let counter =
                    CounterVec::new(Opts::new(metric.name, metric.help), metric.label_names)
                        .unwrap();
                match self.prometheus.registry.register(Box::new(counter.clone())) {
                    Ok(()) => {
                        self.counters_vecs
                            .write()
                            .unwrap()
                            .insert(metric.name.to_string(), counter);
                        Ok(())
                    }
                    Err(e) => {
                        if let prometheus::Error::AlreadyReg = e {
                            log::info!("Metric {:?} is already registered.", metric.name);
                            Ok(())
                        } else {
                            log::error!("Failed to register metric {:?}. {:?}", metric.name, e);
                            Err(Error::Prometheus(e))
                        }
                    }
                }
            }
            Kind::IntGaugeVec => {
                let gauge =
                    IntGaugeVec::new(Opts::new(metric.name, metric.help), metric.label_names)
                        .unwrap();
                match self.prometheus.registry.register(Box::new(gauge.clone())) {
                    Ok(()) => {
                        self.int_gauges_vecs
                            .write()
                            .unwrap()
                            .insert(metric.name.to_string(), gauge);
                        Ok(())
                    }
                    Err(e) => {
                        if let prometheus::Error::AlreadyReg = e {
                            log::info!("Metric {:?} is already registered.", metric.name);
                            Ok(())
                        } else {
                            log::error!("Failed to register metric {:?}. {:?}", metric.name, e);
                            Err(Error::Prometheus(e))
                        }
                    }
                }
            }
            Kind::IntCounterVec => {
                let counter =
                    IntCounterVec::new(Opts::new(metric.name, metric.help), metric.label_names)
                        .unwrap();
                match self.prometheus.registry.register(Box::new(counter.clone())) {
                    Ok(()) => {
                        self.int_counters_vecs
                            .write()
                            .unwrap()
                            .insert(metric.name.to_string(), counter);
                        Ok(())
                    }
                    Err(e) => {
                        if let prometheus::Error::AlreadyReg = e {
                            log::info!("Metric {:?} is already registered.", metric.name);
                            Ok(())
                        } else {
                            log::error!("Failed to register metric {:?}. {:?}", metric.name, e);
                            Err(Error::Prometheus(e))
                        }
                    }
                }
            }
            Kind::IntCounter => {
                let counter = IntCounter::new(metric.name, metric.help).unwrap();
                match self.prometheus.registry.register(Box::new(counter.clone())) {
                    Ok(()) => {
                        self.int_counters
                            .write()
                            .unwrap()
                            .insert(metric.name.to_string(), counter);
                        Ok(())
                    }
                    Err(e) => {
                        if let prometheus::Error::AlreadyReg = e {
                            log::info!("Metric {:?} is already registered.", metric.name);
                            Ok(())
                        } else {
                            log::error!("Failed to register metric {:?}. {:?}", metric.name, e);
                            Err(Error::Prometheus(e))
                        }
                    }
                }
            }
        }
    }
    fn with_metric_configs<'a>(&self, metrics: &'a [MetricConfig<'a>]) -> Result<(), Error> {
        for metric in metrics {
            match self.with_metric_config(metric) {
                Ok(()) => (),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl Registrar {
    pub fn new(prometheus: Arc<PrometheusMetrics>) -> Self {
        Self {
            prometheus,
            ..Default::default()
        }
    }
    // Int
    pub fn inc_int_counter(&self, key: &str) {
        let mut counters = self.int_counters.write().unwrap();
        let counter = match counters.get_mut(key) {
            Some(r) => r,
            None => return,
        };

        counter.inc()
    }
    pub fn inc_int_counter_vec_mut(&self, key: &str, labels: &[&str]) {
        let mut counters = self.int_counters_vecs.write().unwrap();
        let counter = match counters.get_mut(key) {
            Some(r) => r,
            None => return,
        };

        counter.with_label_values(labels).inc()
    }
    pub fn inc_by_int_counter_vec_mut(&self, key: &str, labels: &[&str], value: u64) {
        let mut counters = self.int_counters_vecs.write().unwrap();
        let counter = match counters.get_mut(key) {
            Some(r) => r,
            None => return,
        };

        counter.with_label_values(labels).inc_by(value)
    }

    pub fn set_int_gauge_vec_mut(&self, key: &str, labels: &[&str], value: i64) {
        let mut gauges = self.int_gauges_vecs.write().unwrap();
        let gauge = match gauges.get_mut(key) {
            Some(r) => r,
            None => return,
        };

        gauge.with_label_values(labels).set(value)
    }
    // Floating
    pub fn inc_counter_vec_mut(&self, key: &str, labels: &[&str]) {
        let mut counters = self.counters_vecs.write().unwrap();
        let counter = match counters.get_mut(key) {
            Some(r) => r,
            None => return,
        };

        counter.with_label_values(labels).inc()
    }
    pub fn set_gauge_vec_mut(&self, key: &str, labels: &[&str], value: f64) {
        let mut gauges = self.gauges_vecs.write().unwrap();
        let gauge = match gauges.get_mut(key) {
            Some(r) => r,
            None => return,
        };

        gauge.with_label_values(labels).set(value)
    }
}
