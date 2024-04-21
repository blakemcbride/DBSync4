package dbsync;

import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class ParsePostgres {

	public static Schema parse_postgres_schema(String file) {
		Schema schema = new Schema(Schema.Postgres);
		FileRead fp = new FileRead(file);
		char c, nc;
		while ((char) 0 != (c = fp.readc())) {
			if (is_space(c))
				continue;
			nc = fp.peekc();
			if (c == '-' && nc == '-') {
				skip_to_eol(fp);
				continue;
			}
			String word = get_word(c, fp);
			switch (word) {
				case "SET":
					parse_set(fp);
					break;
				case "COMMENT": {
					ISchemaElement res = parse_comment(fp);
					if (res != null)
						schema.addLast(res);
					break;
				}
				case "CREATE": {
					ISchemaElement res = parse_create(fp);
					if (res != null)
						schema.addLast(res);
					break;
				}
				case "ALTER": {
					ISchemaElement res = parse_alter(fp);
					if (res != null)
						schema.addLast(res);
					break;
				}
				case "REVOKE":
					parse_revoke(fp);
					break;
				case "GRANT":
					parse_grant(fp);
					break;
				case "SELECT":
					parse_select(fp);
					break;
				default:
					System.err.println("Unexpected keyword " + word);
					break;
			}
		}
		fp.Close();
		return schema;
	}

	private static void parse_set(FileRead fp) {
		eat_statement(fp);
	}

	private static void parse_select(FileRead fp) {
		eat_statement(fp);
	}

	private static ISchemaElement parse_comment(FileRead fp) {
		String word, type, what, on = "";
		get_word(fp); // "ON"
		type = get_word(fp);
		what = get_word(fp);
		if (type.equals("CONSTRAINT")) {
			get_word(fp); // "ON"
			on = get_word(fp);
			get_word(fp);  //  "IS"
			word = get_word(fp);  //  comment
			get_word(fp);  //  ";"
			return new Comment(type, what, on, word);
		} else {
			get_word(fp);  // "IS"
			word = get_word(fp);  // comment
			get_word(fp); // ";"
			return new Comment(type, what, on, word);
		}
	}

	private static ISchemaElement parse_create(FileRead fp) {
		String word = get_word(fp);
		if (word.equals(""))
			return null;
		switch (word) {
			case "TABLE": {
				String tname = Main.remove_public(get_word(fp));
				Table table = new Table(tname);
				get_word(fp);  //  eat "("

				while (true) {
					word = get_word(fp);
					if (word.equals(""))
						return table;
					if (word.equals(")")) {
						get_word(fp);  //  eat final ";"
						return table;
					}
					if (word.equals("CONSTRAINT")) {
						Check con = parse_constraint(fp, tname);
						table.add_column(con);
					} else {
						Column col = parse_column(fp, word, tname);
						table.add_column(col);
					}
				}
			}
			case "INDEX": {
				String iname, tname;
				LinkedList<String> fields;
				iname = get_word(fp);
				word = get_word(fp);  // "ON"

				tname = get_word(fp);
				word = get_word(fp); // "USING"

				word = get_word(fp); // "btree"

				word = get_word(fp); // "("

				fields = get_field_list(fp);

				word = "";
				while (!word.equals(";"))  // ignore the rest of the statement
					word = get_word(fp);

				return new Index(iname, tname, false, "INDEX", true, fields);
			}
			case "UNIQUE": {
				String iname, tname;
				LinkedList<String> fields;
				word = get_word(fp);  // "INDEX"

				iname = get_word(fp);
				word = get_word(fp);  // "ON"

				tname = get_word(fp);
				word = get_word(fp); // "USING"

				word = get_word(fp); // "btree"

				word = get_word(fp); // "("

				fields = get_field_list(fp);

				word = "";
				while (!word.equals(";"))  // ignore the rest of the statement
					word = get_word(fp);

				return new Index(iname, tname, false, "UNIQUE", true, fields);
			}
			default:
				return eat_statement(fp);
		}
	}

	private static Column parse_column(FileRead fp, String word, String tname) {
		Column col = new Column(Schema.Postgres, word, tname);
		word = get_word(fp);  //  first word in type
		switch (word) {
			case "character":
				return parse_column_character(fp, col, word);
			case "integer":
			case "smallint":
			case "bigint":
			case "real":
			case "double":
			case "serial":
			case "bigserial":
				return parse_column_number(fp, col, word);
			case "timestamp":
			case "date":
			case "time":
				return parse_column_timestamp(fp, col, word);
			case "text":
				return parse_column_text(fp, col, word);
			case "bytea":
				return parse_column_bytea(fp, col, word);
			default:
				return col;
		}
	}

	private static Column parse_column_character(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast(word); // "character"
		word = get_word(fp);
		if (word.equals("varying")) {
			lst.addLast(word);
			word = get_word(fp);  //  expecting "("
		}
		lst.addLast(word);  //  the "("
		lst.addLast(get_word(fp)); //  the number in the parenthesis
		lst.addLast(get_word(fp)); // the ")"
		col.set_type(lst);
		word = get_word(fp);
		if (word.equals("DEFAULT")) {
			word = get_word(fp);  //  the whole String
			//				word = word + get_word(fp);  // the default
			//				word = word + get_word(fp); //  the "'"
			col.set_default(word);
			word = get_word(fp);
			if (word.equals(":")) {
				get_word(fp);  //  eat second ":"
				word = get_word(fp);  //  eat the type
				if (word.equals("double"))
					get_word(fp);
				word = get_word(fp);
				if (word.equals("varying"))
					word = get_word(fp);
			}
		}
		if (word.equals("NOT")) {
			get_word(fp);  // the "NULL"
			col.set_not_null(true);
			word = get_word(fp);
		}
		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_number(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast(word); // "integer" or "smallint", etc.
		if (word.equals("double"))
			lst.addLast(get_word(fp));
		col.set_type(lst);
		word = get_word(fp);
		if (word.equals("DEFAULT")) {
			word = get_word(fp);
			if (word.equals("("))
				word = get_word(fp);
			else if (word.charAt(0) == '\'') {
				word = word.substring(1, word.length()-1);
				get_word(fp);  // :
				get_word(fp);  // :
				get_word(fp);  // type
			}
			col.set_default(word);
			word = get_word(fp);
			if (word.equals(")"))
				word = get_word(fp);
		}
		if (word.equals("NOT")) {
			get_word(fp);  //  the "NULL"
			col.set_not_null(true);
			word = get_word(fp);
		}
		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_timestamp(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast(word); // "timestamp"
		word = get_word(fp);
		if (word.equals("without") || word.equals("with")) {
			lst.addLast(word);
			lst.addLast(get_word(fp)); // "time"
			lst.addLast(get_word(fp)); // "zone"
			word = get_word(fp);
		}
		col.set_type(lst);
		if (word.equals("DEFAULT")) {
			col.set_default(get_word(fp));
			word = get_word(fp);
		}
		if (word.equals("NOT")) {
			get_word(fp); // the "NULL"
			col.set_not_null(true);
			word = get_word(fp);
		}
		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_text(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast(word); // "text"
		col.set_type(lst);
		word = get_word(fp);
		if (word.equals("NOT")) {
			get_word(fp); // the "NULL"
			col.set_not_null(true);
			word = get_word(fp);
		}
		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_bytea(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast(word); // "bytea"
		col.set_type(lst);
		word = get_word(fp);
		if (word.equals("DEFAULT")) {
			col.set_default(get_word(fp));
			word = get_word(fp);
		}
		if (word.equals("NOT")) {
			get_word(fp);  //  the "NULL"
			col.set_not_null(true);
			word = get_word(fp);
		}
		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Check parse_constraint(FileRead fp, String tname) {
		String word = get_word(fp);
		Check con = new Check(Schema.Postgres, word, tname);
		LinkedList<String> lst = new LinkedList<String>();
		int n = 0;  //  number of left parens

		get_word(fp);  //  "CHECK"
		word = get_word(fp); //  should be "("
		if (word.equals("("))
			n++;
		lst.addLast(word);
		word = get_word(fp);
		while (true) {
			if (word.equals("("))
				n++;
			else if (word.equals(")"))
				n--;
			if (word.equals(":")) {
				get_word(fp);  //  should be the next ":"
				word = get_word(fp);  //  should be the type
				if (word.equals("double"))
					get_word(fp);
				word = get_word(fp);
				if (word.equals("varying"))
					word = get_word(fp);
			} else {
				lst.addLast(word);
				if (n == 0)
					break;
				word = get_word(fp);
			}
		}
		con.set_constraint(lst);
		word = get_word(fp);  // either "," or ")"
		if (word.equals(")"))
			fp.pushc(')');
		return con;
	}

	private static ISchemaElement parse_alter(FileRead fp) {
		String word = get_word(fp);  // "TABLE"
		boolean only = false;
		String tname;
		String cname;
		LinkedList<String> fields;
		LinkedList<String> fields2;
		String tname2;

		word = Main.remove_public(get_word(fp));
		if (word.equals("ONLY")) {
			only = true;
			word = Main.remove_public(get_word(fp));
		}
		tname = word;  //  table name
		word = get_word(fp);  //  either "ADD" or "OWNER"
		if (word.equals("ADD")) {
			word = get_word(fp);  //  "CONSTRAINT"
			cname = get_word(fp);  //  constraint name
			word = get_word(fp);   //  "PRIMARY", "UNIQUE" or "FOREIGN"
			switch (word) {
				case "PRIMARY":
					word = get_word(fp);  //  "KEY"

					word = get_word(fp); //  "("

					fields = get_field_list(fp);
					word = get_word(fp);  //  ";"

					return new Index(cname, tname, only, "PRIMARY KEY", false, fields);
				case "UNIQUE":
					word = get_word(fp);  // "("

					fields = get_field_list(fp);
					word = get_word(fp);  //  ";"

					return new Index(cname, tname, only, "UNIQUE", false, fields);
				case "FOREIGN":
					word = get_word(fp);  //  "KEY"

					word = get_word(fp);  //  "("

					fields = get_field_list(fp);
					word = get_word(fp);  //  "REFERENCES"

					tname2 = get_word(fp);
					word = get_word(fp);  //  "("

					fields2 = get_field_list(fp);
					word = get_word(fp);  //  ";"

					return new ForeignKey(cname, tname, only, fields, tname2, fields2);
				default:
					return eat_statement(fp);
			}
		} else
			return eat_statement(fp);
	}

	private static LinkedList<String> get_field_list(FileRead fp) {
		LinkedList<String> lst = new LinkedList<String>();
		String word, tmp, pword = "";

		word = get_word(fp);
		while (true) {
			if (word.equals(")") || word.equals(""))
				return lst;
			if (!word.equals(","))
				if (word.equals("upper") || word.equals("lower")) {
					word += get_word(fp);  //  "("
					tmp = get_word(fp);  //  "(" or name
					if (tmp.equals("("))
						word += get_word(fp);  //  column name
					else
						word += tmp;
					word += get_word(fp);  //  ")"
					tmp = get_word(fp);  //  ":" or no type
					if (tmp.equals(":")) {
						get_word(fp);  //  ":"
						tmp = get_word(fp);   //  the type
						if (tmp.equals("double"))
							get_word(fp);
						tmp = get_word(fp);    //  "varying" or ")"
						if (tmp.equals("varying"))
							get_word(fp);
						lst.addLast(word);
						word = get_word(fp);
					} else {
						lst.addLast(word);
						word = tmp;
					}
					pword = "xx";  //  anything other than a comma
				} else if (!pword.equals(",")  &&  !pword.equals("")  &&  (word.equals("DESC") || word.equals("ASC"))) {
					tmp = lst.removeLast();
					tmp = tmp + " " + word;
					lst.addLast(tmp);
					pword = word;
					word = get_word(fp);
				} else {
					lst.addLast(word);
					pword = word;
					word = get_word(fp);
				}
			else {  //  is comma
				pword = word;
				word = get_word(fp);
			}
		}
	}

	private static void parse_revoke(FileRead fp) {
		eat_statement(fp);
	}

	private static void parse_grant(FileRead fp) {
		eat_statement(fp);
	}

	private static ISchemaElement eat_statement(FileRead fp) {
		String word;

		do {
			word = get_word(fp);
		} while (!word.equals("") && !word.equals(";"));
		return null;
	}

	private static String get_word(FileRead fp) {
		return get_word((char) 0, fp);
	}

	private static String get_word(char c, FileRead fp) {
		StringBuilder word = new StringBuilder();
		if (c == '"')
			c = (char) 0;
		if (is_sep(c))
			word.append(c);
		else {
			char quotec = c == '\'' ? c : (char) 0;
			char nc;
			if (c != (char) 0)
				word.append(c);
			while (true) {
				c = fp.readc();
				if (c == (char) 0)
					break;
				if (c == '"')
					continue;
				if (quotec != (char) 0) {
					nc = fp.peekc();
					if (c == quotec && nc != quotec) {
						word.append(c);
						break;
					}
					word.append(c);
					if (c == quotec) {
						word.append(c);
						fp.readc();  //  throw it away
					}
					continue;
				}
				if (c == '\'')
					if (word.length() == 0) {
						word.append(c);
						quotec = c;
						continue;
					} else {
						fp.pushc(c);
						break;
					}
				if (is_sep(c))
					if (word.length() == 0) {
						word.append(c);
						break;
					} else {
						fp.pushc(c);
						break;
					}
				if (is_space(c))
					if (word.length() == 0)
						continue;
					else
						break;
				word.append(c);
			}
		}
//		System.out.println("word: \"" + word + "\"");
		return word.toString();
	}

	private static boolean is_sep(char c) {
		return c == '(' ||
				c == ')' ||
				c == ';' ||
				c == ',' ||
				c == ':' ||
				c == '=' ||
				c == '!' ||
				c == '<' ||
				c == '>';
	}

	private static boolean is_eol(char c) {
		return c == '\r' || c == '\n';
	}

	private static boolean is_space(char c) {
		return c == ' ' || c == '\t' || is_eol(c);
	}

	private static void skip_to_eol(FileRead fp) {
		char c;
		while ((char) 0 != (c = fp.readc()))
			if (is_eol(c))
				break;
	}
}
