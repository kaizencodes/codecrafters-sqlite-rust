#[derive(Debug, PartialEq)]
struct SelectStatement<'t> {
    args: Vec<SelectArg<'t>>,
    table: &'t str,
}

#[derive(Debug, PartialEq)]
enum SelectArg<'t> {
    LITERAL(&'t str),
    COUNT(&'t str),
}

#[derive(Debug, PartialEq)]
struct CreateTableStatement<'t> {
    args: Vec<CreateTableArg<'t>>,
    table: &'t str,
}

#[derive(Debug, PartialEq)]
struct CreateTableArg<'t> {
    column: &'t str,
    //TODO: datatype: &'t str
}

peg::parser! {
  grammar sql_parser() for str {
    // General definitions
    rule literal() -> &'input str
        = val:$(['a'..='z' | 'A'..='Z']+) { val }

    rule _ = [' ' | '\n' | '\t']+
    rule __ = [' ' | '\n' | '\t']*

    rule commasep<T>(x: rule<T>) -> Vec<T> = v:(x() ** (_? "," _)) ","? {v}
    rule parenthesised<T>(x: rule<T>) -> T = "(" v:x() ")" {v}
    rule i(literal: &'static str) = input:$([_]*<{literal.len()}>) {? if input.eq_ignore_ascii_case(literal) { Ok(()) } else { Err(literal) } }

    // SELECT
    pub rule select_statement() -> SelectStatement<'input>
        = i("SELECT") _ args:commasep(<select_arg()>) _ i("FROM") _ table:literal() __ ";" {
            SelectStatement{args, table }
        }

    rule select_arg() -> SelectArg<'input>
        = arg:(count() / select_literal()) { arg }

    rule count() -> SelectArg<'input>
        = i("COUNT") __ arg:parenthesised(<(literal() / $("*"))>) {
            SelectArg::COUNT(arg)
        }

    rule select_literal() -> SelectArg<'input>
        = arg:(literal() / $("*")) { SelectArg::LITERAL(arg) }


    // CREATE TABLE
    pub rule create_table_statement() -> CreateTableStatement<'input>
        = i("CREATE") _ i("TABLE") _ table:literal() _ "(" _ args:commasep(<create_table_arg()>) _ ")" _? ";"? {
            CreateTableStatement{args, table}
        }

    rule create_table_arg() -> CreateTableArg<'input>
        = column:literal() _ (literal() ** _) { CreateTableArg{column} }
  }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement() {
        let input = "SELECT name FROM table;";
        let expected = SelectStatement {
            args: vec![SelectArg::LITERAL("name")],
            table: "table",
        };

        let result = sql_parser::select_statement(input).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_statement_multi_select() {
        let input = "SELECT name, id FROM table;";
        let expected = SelectStatement {
            args: vec![SelectArg::LITERAL("name"), SelectArg::LITERAL("id")],
            table: "table",
        };

        let result = sql_parser::select_statement(input).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_statement_whitespace() {
        let input = "SELECT          name  ,   id       FROM
            table
            ;";
        let expected = SelectStatement {
            args: vec![SelectArg::LITERAL("name"), SelectArg::LITERAL("id")],
            table: "table",
        };

        let result = sql_parser::select_statement(input).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_statement_wildcard() {
        let input = "SELECT * FROM table;";
        let expected = SelectStatement {
            args: vec![SelectArg::LITERAL("*")],
            table: "table",
        };

        let result = sql_parser::select_statement(input).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_statement_count() {
        let input = "SELECT COUNT(name) FROM table;";
        let expected = SelectStatement {
            args: vec![SelectArg::COUNT("name")],
            table: "table",
        };

        let result = sql_parser::select_statement(input).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_statement_create() {
        let input = "CREATE TABLE apples
                    (
                       	id integer primary key autoincrement,
                       	name text,
                       	color text
                    )";
        let expected = CreateTableStatement {
            args: vec![
                CreateTableArg { column: "id" },
                CreateTableArg { column: "name" },
                CreateTableArg { column: "color" },
            ],
            table: "apples",
        };

        let result = sql_parser::create_table_statement(input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }
}
