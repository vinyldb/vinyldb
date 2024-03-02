use crate::{
    catalog::{catalog::Catalog, schema::Schema},
    error::{Error, Result},
    logical_plan::LogicalPlan,
    physical_plan::{
        create_table::CreateTableExec,
        describe_table::DescribeTableExec,
        explain::ExplainExec,
        show_tables::ShowTablesExec,
        tuple::{Tuple, TupleStream},
        Executor,
    },
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

/// Context for everything
///
/// 1. Planing
/// 2. Executing
pub struct Context {
    pub catalog: Catalog,
}

impl Context {
    /// Create a new [`Context`].
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
        }
    }

    pub fn statement_to_logical_plan(
        &self,
        statement: &Statement,
    ) -> Result<LogicalPlan> {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let pk = 0;
                let schema = Schema::new(columns.into_iter().map(|col| {
                    (
                        col.name.value.clone(),
                        col.data_type.clone().try_into().unwrap(),
                    )
                }))?;

                Ok(LogicalPlan::CreateTable {
                    name: name.to_string(),
                    schema,
                    pk,
                })
            }
            Statement::Explain { statement, .. } => {
                let plan = Box::new(self.statement_to_logical_plan(statement)?);
                Ok(LogicalPlan::Explain { plan })
            }
            Statement::ShowTables { .. } => Ok(LogicalPlan::ShowTables),
            Statement::ExplainTable { table_name, .. } => {
                Ok(LogicalPlan::DescribeTable {
                    name: table_name.to_string(),
                })
            }
            _ => Err(Error::NotImplemented),
        }
    }

    pub fn create_logical_plan<S: AsRef<str>>(
        &self,
        sql: S,
    ) -> Result<LogicalPlan> {
        let sql = sql.as_ref();
        let statement = Parser::parse_sql(&DIALECT, sql)
            .map(|mut asts| asts.pop().unwrap())?;

        self.statement_to_logical_plan(&statement)
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
            _ => return Err(Error::NotImplemented),
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
