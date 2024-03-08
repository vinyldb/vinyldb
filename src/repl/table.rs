use crate::{catalog::schema::Schema, data::tuple::Tuple};
use comfy_table::{
    Table,
    TableComponent::{
        BottomBorder, BottomBorderIntersections, BottomLeftCorner,
        BottomRightCorner, HeaderLines, HorizontalLines, LeftBorder,
        LeftBorderIntersections, LeftHeaderIntersection,
        MiddleHeaderIntersections, MiddleIntersections, RightBorder,
        RightBorderIntersections, RightHeaderIntersection, TopBorder,
        TopBorderIntersections, TopLeftCorner, TopRightCorner, VerticalLines,
    },
};
use std::fmt::Display;

/// Create a new table, with our custom style.
fn new_table() -> Table {
    let mut table = Table::new();
    table.set_style(TopLeftCorner, '┌');
    table.set_style(TopRightCorner, '┐');
    table.set_style(BottomLeftCorner, '└');
    table.set_style(BottomRightCorner, '┘');

    table.set_style(HeaderLines, '─');
    table.set_style(VerticalLines, '│');
    table.set_style(HorizontalLines, '─');

    table.set_style(LeftBorderIntersections, '├');
    table.set_style(RightBorderIntersections, '┤');
    table.set_style(TopBorderIntersections, '┬');
    table.set_style(BottomBorderIntersections, '┴');

    table.set_style(MiddleIntersections, '┼');

    table.set_style(LeftHeaderIntersection, '├');
    table.set_style(MiddleHeaderIntersections, '┼');
    table.set_style(RightHeaderIntersection, '┤');

    table.set_style(LeftBorder, '│');
    table.set_style(RightBorder, '│');
    table.set_style(TopBorder, '─');
    table.set_style(BottomBorder, '─');

    table
}

/// Return a displayable return.
pub fn display(schema: &Schema, result: Vec<Tuple>) -> impl Display {
    let mut table = new_table();

    table.set_header(
        schema
            .columns()
            .map(|(name, datatype)| format!("{name}({datatype})"))
            .collect::<Vec<_>>(),
    );
    for tuple in result {
        table.add_row(
            tuple
                .iter()
                .map(|data| format!("{data}"))
                .collect::<Vec<_>>(),
        );
    }

    table
}
