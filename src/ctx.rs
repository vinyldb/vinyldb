use crate::{
    catalog::Catalog,
    config::Config,
    data::tuple::{Tuple, TupleStream},
    error::Result,
    logical_plan::LogicalPlan,
    physical_plan::{
        create_table::CreateTableExec, describe_table::DescribeTableExec,
        explain::ExplainExec, filter::FilterExec, insert::InsertExec,
        limit::LimitExec, show_tables::ShowTablesExec,
        table_scan::TableScanExec, Executor,
    },
    storage_engine::StorageEngine,
    utils::data_dir,
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

/// Context for everything
///
/// 1. Planing
/// 2. Executing
#[derive(Debug)]
pub struct Context {
    pub config: Config,
    pub catalog: Catalog,
    pub storage: StorageEngine,
}

impl Context {
    /// Create a new [`Context`].
    pub fn new() -> Result<Self> {
        let data_dir = data_dir();
        std::fs::create_dir_all(data_dir.as_path())?;

        let config = Config::new();
        let catalog = Catalog::new();
        let storage = StorageEngine::new()?;
        let ctx = Self {
            config,
            catalog,
            storage,
        };

        Ok(ctx)
    }

    pub fn statement_to_logical_plan(
        &self,
        statement: Statement,
    ) -> Result<LogicalPlan> {
        crate::plan::statement_to_logical_plan(&self.catalog, statement)
    }

    pub fn sql_to_statement<S: AsRef<str>>(&self, sql: S) -> Result<Statement> {
        let sql = sql.as_ref();
        let statement = Parser::parse_sql(&DIALECT, sql)
            .map(|mut asts| asts.pop().unwrap())?;

        Ok(statement)
    }

    pub fn create_logical_plan<S: AsRef<str>>(
        &self,
        sql: S,
    ) -> Result<LogicalPlan> {
        let statement = self.sql_to_statement(sql)?;
        self.statement_to_logical_plan(statement)
    }

    pub fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Box<dyn Executor>> {
        let plan: Box<dyn Executor> = match logical_plan {
            LogicalPlan::CreateTable { name, schema, pk } => Box::new(
                CreateTableExec::new(name.clone(), schema.clone(), *pk),
            ),
            LogicalPlan::Explain { plan } => {
                let plan = self.create_physical_plan(plan)?;
                Box::new(ExplainExec::new(plan))
            }
            LogicalPlan::ShowTables => Box::new(ShowTablesExec),
            LogicalPlan::DescribeTable { name } => {
                Box::new(DescribeTableExec::new(name.clone()))
            }
            LogicalPlan::Insert { table, rows } => {
                Box::new(InsertExec::new(table.clone(), rows.to_vec()))
            }
            LogicalPlan::TableScan { name } => {
                let table_catalog = self.catalog.get_table(name)?;
                let schema = table_catalog.schema().clone();
                Box::new(TableScanExec::new(name.clone(), schema))
            }
            LogicalPlan::Filter { predicate, input } => {
                let input = self.create_physical_plan(input)?;
                Box::new(FilterExec::new(predicate.clone(), input))
            }
            LogicalPlan::Limit { fetch, input } => {
                let input = self.create_physical_plan(input)?;
                Box::new(LimitExec::new(*fetch, input))
            }
        };

        Ok(plan)
    }

    pub fn execute(
        &mut self,
        physical_plan: &dyn Executor,
    ) -> Result<TupleStream> {
        physical_plan.execute(self)
    }

    pub fn collect(
        &mut self,
        physical_plan: &dyn Executor,
    ) -> Result<Vec<Tuple>> {
        let iter = self.execute(physical_plan)?;
        Ok(iter.into_iter().collect())
    }
}
