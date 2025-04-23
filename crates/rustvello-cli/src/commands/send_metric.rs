// crates/rustvello-cli/src/commands/register_metric.rs

use clap::Args;

use super::utils::create_poet_client;
use crate::common::CommonArgs;
use rustvello_core::{time, MuseResult, Transport};
use rustvello_proto::types::*;
use rustvello_proto::{metric_id_from_code, MetricPayload};

#[derive(Args)]
pub struct SendMetricArgs {
    #[clap(flatten)]
    pub common: CommonArgs,
    /// Element ID
    #[arg(short, long)]
    pub element_id: ElementId,
    /// Metric ID
    #[arg(short, long)]
    pub metric_code: String,
    /// Metric value
    #[arg(short, long)]
    pub value: MetricValue,
}

pub async fn execute(args: SendMetricArgs) -> MuseResult<()> {
    let client = create_poet_client(&args.common.poet_url);

    let payload = MetricPayload {
        time: time::utc_now_i64(),
        element_id: args.element_id,
        metric_ids: vec![metric_id_from_code(&args.metric_code)],
        values: vec![Some(args.value)],
    };

    client.send_metrics(vec![payload], None).await
}
