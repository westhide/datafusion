use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{internal_err, Statistics},
    error::Result,
    execution::{
        context::{QueryPlanner, SessionState},
        SendableRecordBatchStream, TaskContext,
    },
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_expr::PhysicalSortExpr,
    physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning},
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

pub struct QueryPlannerExt;

#[async_trait]
impl QueryPlanner for QueryPlannerExt {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = Arc::new(PlannerExt {});
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![planner]);

        physical_planner.create_physical_plan(logical_plan, session_state).await
    }
}

pub struct PlannerExt;

#[async_trait]
impl ExtensionPlanner for PlannerExt {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(if let Some(node) = node.as_any().downcast_ref::<PlanNodeExt>() {
            Some(Arc::new(ExtensionExec { input: physical_inputs[0].clone() }))
        } else {
            None
        })
    }
}

pub struct PlanNodeExt;

#[derive(Debug)]
pub struct ExtensionExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for ExtensionExec {
    fn fmt_as(&self, ty: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match ty {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ExecutionPlan")
            },
        }
    }
}

#[async_trait]
impl ExecutionPlan for ExtensionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self { input: children[0].clone() }))
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("ExecutionPlan invalid partition {partition}");
        }

        todo!()
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
