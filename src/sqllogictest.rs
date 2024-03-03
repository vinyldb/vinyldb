//! To make VinylDB can use Sqllogictest.

use crate::{data::types::DataType, error::Error, VinylDB};
use sqllogictest::{ColumnType, DBOutput, DB};
use std::ops::Deref;

impl ColumnType for DataType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Bool),
            'T' => Some(Self::String),
            'I' => Some(Self::Int64),
            'F' => Some(Self::Float64),
            _ => unreachable!(),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Bool => 'B',
            Self::String => 'S',
            Self::Int64 => 'I',
            Self::Float64 => 'F',
            Self::Timestamp => todo!(),
        }
    }
}

impl DB for VinylDB {
    type Error = Error;
    type ColumnType = DataType;

    fn run(
        &mut self,
        sql: &str,
    ) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let logical_plan = self.create_logical_plan(sql)?;
        let physical_plan = self.create_physical_plan(&logical_plan)?;
        let result = self.collect(physical_plan.deref())?;

        if result.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }

        let types = result.first().unwrap().datatypes();

        let rows = result
            .into_iter()
            .map(|tuple| {
                tuple
                    .iter()
                    .map(|data| data.to_string())
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<_>>();

        Ok(DBOutput::Rows { types, rows })
    }
}
