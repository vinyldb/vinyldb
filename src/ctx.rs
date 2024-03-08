use crate::{
    catalog::Catalog,
    config::{Config, ConfigBuilder},
    data::tuple::{Tuple, TupleStream},
    error::Result,
    logical_plan::LogicalPlan,
    physical_plan::{
        create_table::CreateTableExec, describe_table::DescribeTableExec,
        explain::ExplainExec, filter::FilterExec, insert::InsertExec,
        limit::LimitExec, one_row_placeholder::OneRowPlaceholderExec,
        projection::ProjectionExec, show_tables::ShowTablesExec,
        table_scan::TableScanExec, Executor,
    },
    storage_engine::StorageEngine,
};
use camino::Utf8Path;
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
    pub fn new<P: AsRef<Utf8Path>>(data_path: P) -> Result<Self> {
        let data_path = data_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_path)?;

        let config = ConfigBuilder::default()
            .show_ast(false)
            .timer(true)
            .data_path(data_path)
            .build()
            .unwrap();
        let storage = StorageEngine::new(&config)?;
        let catalog = Catalog::new(&storage)?;
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
            LogicalPlan::CreateTable {
                name,
                schema,
                pk,
                sql,
            } => Box::new(CreateTableExec::new(
                name.clone(),
                schema.clone(),
                *pk,
                sql.clone(),
            )),
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
            LogicalPlan::Limit {
                offset,
                limit,
                input,
            } => {
                let input = self.create_physical_plan(input)?;
                Box::new(LimitExec::new(*offset, *limit, input))
            }
            LogicalPlan::Projection {
                expr,
                schema,
                input,
            } => {
                let input = self.create_physical_plan(input)?;
                Box::new(ProjectionExec::new(
                    expr.clone(),
                    schema.clone(),
                    input,
                ))
            }
            LogicalPlan::OneRowPlaceholder => Box::new(OneRowPlaceholderExec),
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
