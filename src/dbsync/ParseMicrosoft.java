package dbsync;

import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class ParseMicrosoft {

	public static Schema parse_microsoft_schema(String file) {
		Schema schema = new Schema(Schema.Microsoft);
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
			if (c == '/' && nc == '*') {
				skip_comment(fp);
				continue;
			}
			String word = get_word(c, fp);
			if (word.equals("SET"))
				parse_set(fp);
			else if (word.equals("COMMENT")) {
				ISchemaElement res = parse_comment(fp);
				if (res != null)
					schema.addLast(res);
			} else if (word.equals("CREATE")) {
				ISchemaElement res = parse_create(fp, schema);
				if (res != null)
					schema.addLast(res);
			} else if (word.equals("ALTER"))
				parse_alter(fp, schema);
			else if (word.equals("REVOKE"))
				parse_revoke(fp);
			else if (word.equals("GRANT"))
				parse_grant(fp);
			else if (word.equals("USE"))
				parse_use(fp);
			else
				System.err.println("Unexpected keyword " + word);
		}
		fp.Close();
		return schema;
	}

	public static void microsoft_inserts_to_postgres(String file) {
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
			if (c == '/' && nc == '*') {
				skip_comment(fp);
				continue;
			}
			String word = get_word(c, fp);
			if (word.equals("INSERT"))
				parse_insert(fp);
			else if (word.equals("GO"))
				continue;
			else if (word.equals("print")) {
				word = get_word(fp);  //  the text
			} else {
				System.err.println("Unexpected keyword " + word);
				System.err.println("Aborting conversion.");
				break;
			}
		}
		fp.Close();
	}

	private static void parse_insert(FileRead fp) {
		String word;
		boolean prev = false;
		System.out.print("INSERT INTO ");
		word = get_word(fp);	//  dbo.table
		word = drop_owner(word);
		System.out.print(word + " (");
		word = get_word(fp);	//  (
		word = get_word(fp);	//  first column name
		while (!word.equals(")")) {
			if (word.equals(",")) {
				word = get_word(fp);
				continue;
			}
			if (prev)
				System.out.print(", ");
			System.out.print(word);
			prev = true;
			word = get_word(fp);	//  next column name, comma or )
		}
		word = get_word(fp);	//  VALUES
		word = get_word(fp);	//  (
		System.out.print(") VALUES (");
		prev = false;
		word = get_word(fp);	//  first value
		while (!word.equals(")")) {
			if (word.equals(",")) {
				word = get_word(fp);
				continue;
			}
			if (prev)
				System.out.print(", ");
			if (word.equals("CAST"))
				parse_cast(fp);
			else
				System.out.print(word);
			prev = true;
			word = get_word(fp);	//  next value, comma or )
		}
		System.out.println(");");
	}

	private static String parse_cast(FileRead fp) {
		String word = get_word(fp);	//  (
		String tv = get_word(fp);	//  timevalue
		word = get_word(fp);	//  AS
		word = get_word(fp);	//  DateTime
		word = get_word(fp);	//  )
		return "0";
	}

	private static String drop_owner(String db) {
		int idx = db.indexOf('.');
		if (idx == -1)
			return db;
		return db.substring(idx+1);
	}

	private static void parse_set(FileRead fp) {
		eat_statement(fp);
	}

	private static void parse_use(FileRead fp) {
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

	private static ISchemaElement parse_create(FileRead fp, Schema schema) {
		String word = get_word(fp);
		if (word.equals(""))
			return null;
		if (word.equals("TABLE")) {
			String tname = drop_dbo(get_word(fp));
			Table table = new Table(tname);
			get_word(fp);  //  eat "("
			while (true) {
				word = get_word(fp);
				if (word.equals(""))
					return table;
				if (word.equals(")")) {
					eat_statement(fp);  //  eat till the "GO"
					return table;
				}
				if (word.equals("CONSTRAINT"))
					parse_constraint(fp, tname, table, schema);
				else {
					Column col = parse_column(fp, word, tname);
					table.add_column(col);
				}
			}
		} else if (word.equals("INDEX")) {
			String iname, tname;
			LinkedList<String> fields;
			iname = get_word(fp);
			word = get_word(fp);  // "ON"
			tname = drop_dbo(get_word(fp));
			word = get_word(fp); // "("
			fields = get_field_list(fp);
			word = get_word(fp);
			if (word.equals("WITH")) {
				word = get_word(fp);
				if (!word.equals("FILLFACTOR")) // eat "FILLFACTOR"
				{
					System.err.println("Expected \"FILLFACTOR\" but got " + word);
					System.exit(11);
				}
				get_word(fp);  //  eat "="
				get_word(fp);  //  eat number
				word = get_word(fp);
			}
			if (word.equals("ON")) {
				get_word(fp);  //  eat "PRIMARY"
				word = get_word(fp);  //  "GO"?
				if (word.equals("TEXTIMAGE_ON")) {
					word = get_word(fp);  //  eat "PRIMARY"
					word = get_word(fp);  //  "GO"
				}
			}
			return new Index(iname, tname, false, "INDEX", true, fields);
		} else if (word.equals("UNIQUE")) {
			String iname, tname;
			LinkedList<String> fields;
			word = get_word(fp);  // "INDEX"
			iname = get_word(fp);
			word = get_word(fp);  // "ON"
			tname = drop_dbo(get_word(fp));
			word = get_word(fp); // "("
			fields = get_field_list(fp);
			word = get_word(fp);
			if (word.equals("WITH")) {
				word = get_word(fp);
				if (!word.equals("FILLFACTOR")) // eat "FILLFACTOR"
				{
					System.err.println("Expected \"FILLFACTOR\" but got " + word);
					System.exit(11);
				}
				get_word(fp);  //  eat "="
				get_word(fp);  //  eat number
				word = get_word(fp);
			}
			if (word.equals("ON")) {
				get_word(fp);  //  eat "PRIMARY"
				word = get_word(fp);  //  "GO"?
				if (word.equals("TEXTIMAGE_ON")) {
					word = get_word(fp);  //  eat "PRIMARY"
					word = get_word(fp);  //  "GO"
				}
			}
			return new Index(iname, tname, false, "UNIQUE", true, fields);
		} else
			return eat_statement(fp);
	}

	private static String drop_dbo(String x) {
		if (x.length() > 4 && x.startsWith("dbo."))
			return x.substring(4);
		else
			return x;
	}

	private static Column parse_column(FileRead fp, String word, String tname) {
		Column col = new Column(Schema.Microsoft, word, tname);
		word = get_word(fp);  //  first word in type
		if (word.equals("char") || word.equals("varchar") || word.equals("character"))
			return parse_column_character(fp, col, word);
		else if (word.equals("int") ||
				word.equals("integer") ||
				word.equals("smallint") ||
				word.equals("float") ||
				word.equals("real") ||
				word.equals("double"))
			return parse_column_number(fp, col, word);
		else if (word.equals("timestamp") ||
				word.equals("datetime") ||
				word.equals("date") ||
				word.equals("time"))
			return parse_column_timestamp(fp, col, word);
		else if (word.equals("text"))
			return parse_column_text(fp, col, word);
		else if (word.equals("image"))
			return parse_column_image(fp, col, word);
		else
			return col;
	}

	private static Column parse_column_character(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		if (word.equals("varchar")) {
			lst.addLast("character");
			lst.addLast("varying");
		} else if (word.equals("char"))
			lst.addLast("character");
		else
			lst.addLast(word);
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
		while (!word.equals(")") && !word.equals(","))
			if (word.equals("COLLATE")) {
				get_word(fp);  //  the colating sequence
				word = get_word(fp);
			} else if (word.equals("CONSTRAINT")) {
				get_word(fp);  //  eat constraint name
				word = get_word(fp);
			} else if (word.equals("DEFAULT")) {
				int parens = 0;
				word = get_word(fp);
				while (word.equals("(")) {
					word = get_word(fp);
					parens++;
				}
				col.set_default(word);
				while (parens-- != 0)
					word = get_word(fp);
				word = get_word(fp);
			} else if (word.equals("NOT")) {
				get_word(fp);  // the "NULL"
				col.set_not_null(true);
				word = get_word(fp);
			} else if (word.equals("NULL"))
				word = get_word(fp);


		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_number(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		if (word.equals("int"))
			lst.addLast("integer");
		else if (word.equals("float")) {
			lst.addLast("double");
			lst.addLast("precision");
		} else
			lst.addLast(word);
		if (word.equals("double"))
			lst.addLast(get_word(fp));
		col.set_type(lst);


		word = get_word(fp);
		while (!word.equals(")") && !word.equals(","))
			if (word.equals("CONSTRAINT")) {
				get_word(fp);  //  eat constraint name
				word = get_word(fp);
			} else if (word.equals("DEFAULT")) {
				int parens = 0;
				word = get_word(fp);
				while (word.equals("(")) {
					word = get_word(fp);
					parens++;
				}
				col.set_default(word);
				while (parens-- != 0)
					word = get_word(fp);
				word = get_word(fp);
			} else if (word.equals("NOT")) {
				get_word(fp);  //  the "NULL"
				col.set_not_null(true);
				word = get_word(fp);
			} else if (word.equals("NULL"))
				word = get_word(fp);


		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_timestamp(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast("timestamp"); // "timestamp"
		word = get_word(fp);
		if (word.equals("without")) {
			lst.addLast(word);
			lst.addLast(get_word(fp)); // "time"
			lst.addLast(get_word(fp)); // "zone"
			word = get_word(fp);
		}
		col.set_type(lst);

		while (!word.equals(")") && !word.equals(","))
			if (word.equals("CONSTRAINT")) {
				get_word(fp);  //  eat constraint name
				word = get_word(fp);
			} else if (word.equals("DEFAULT")) {
				int parens = 0;
				word = get_word(fp);
				while (word.equals("(")) {
					word = get_word(fp);
					parens++;
				}
				col.set_default(word);
				while (parens-- != 0)
					word = get_word(fp);
				word = get_word(fp);
			} else if (word.equals("NOT")) {
				get_word(fp); // the "NULL"
				col.set_not_null(true);
				word = get_word(fp);
			} else if (word.equals("NULL"))
				word = get_word(fp);


		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_text(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast(word); // "text"
		col.set_type(lst);

		word = get_word(fp);
		while (!word.equals(")") && !word.equals(","))
			if (word.equals("COLLATE")) {
				get_word(fp);  //  the collating sequence
				word = get_word(fp);
			} else if (word.equals("NOT")) {
				get_word(fp); // the "NULL"
				col.set_not_null(true);
				word = get_word(fp);
			} else if (word.equals("NULL"))
				word = get_word(fp);


		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	private static Column parse_column_image(FileRead fp, Column col, String word) {
		LinkedList<String> lst = new LinkedList<String>();
		lst.addLast("bytea");
		col.set_type(lst);


		word = get_word(fp);
		while (!word.equals(")") && !word.equals(","))
			if (word.equals("CONSTRAINT")) {
				get_word(fp);  //  eat constraint name
				word = get_word(fp);
			} else if (word.equals("DEFAULT")) {
				col.set_default(get_word(fp));
				word = get_word(fp);
			} else if (word.equals("NOT")) {
				get_word(fp);  //  the "NULL"
				col.set_not_null(true);
				word = get_word(fp);
			} else if (word.equals("NULL"))
				word = get_word(fp);


		if (word.equals(")"))
			fp.pushc(')');
		return col;
	}

	//  This is a constraint inside a create table
	private static void parse_constraint(FileRead fp, String tname, Table table, Schema schema) {
		String cname = get_word(fp);
		String word = get_word(fp);

		if (word.equals("CHECK")) {
			Check con = new Check(Schema.Microsoft, cname, tname);
			LinkedList<String> lst = new LinkedList<String>();
			int n = 0;  //  number of left parens

			word = get_word(fp); //  should be "("
			if (word.equals("("))
				n++;
			lst.addLast(word);
			while (true) {
				word = get_word(fp);
				if (word.equals("("))
					n++;
				else if (word.equals(")"))
					n--;
				if (word.equals(":")) {
					get_word(fp);  //  should be the next ":"
					get_word(fp);  //  should be the "bpchar"
				} else {
					lst.addLast(word);
					if (n == 0)
						break;
				}
			}
			con.set_constraint(lst);
			word = get_word(fp);  // either "," or ")"
			if (word.equals(")"))
				fp.pushc(')');
			table.add_column(con);
		} else if (word.equals("PRIMARY"))
			parse_primary_key(fp, cname, tname, schema);
		else if (word.equals("UNIQUE"))
			parse_unique(fp, cname, tname, schema);
		else {
			System.err.println("Expected CONSTRAINT type in a CREATE TABLE - " + word);
			System.exit(11);
		}
	}

	private static String parse_primary_key(FileRead fp, String cname, String tname, Schema schema) {
		LinkedList<String> fields;
		boolean only = false;
		String word = get_word(fp);  //  KEY
		word = get_word(fp);
		if (word.equals("CLUSTERED") || word.equals("NONCLUSTERED"))
			word = get_word(fp);
		//  word expected to be "(" at this point
		fields = get_field_list(fp);
		word = get_word(fp);
		if (word.equals("WITH")) {
			word = get_word(fp);
			if (word.equals("("))
				get_field_list(fp);
			else {
				if (!word.equals("FILLFACTOR")) // eat "FILLFACTOR"
				{
					System.err.println("Expected \"FILLFACTOR\" but got " + word);
					System.exit(11);
				}
				get_word(fp);  //  eat "="
				get_word(fp);  //  eat number
			}
			word = get_word(fp);
		}
		if (word.equals("ON")) {
			get_word(fp);  // eat "PRIMARY"
			word = get_word(fp);
			if (word.equals("TEXTIMAGE_ON")) {
				word = get_word(fp);  //  eat "PRIMARY"
				word = get_word(fp);
			}
		}
		if (word.equals(")"))
			fp.pushc(')');
		schema.addLast(new Index(cname, tname, only, "PRIMARY KEY", false, fields));
		return word;
	}

	private static String parse_unique(FileRead fp, String cname, String tname, Schema schema) {
		LinkedList<String> fields;
		boolean only = false;
		String word = get_word(fp);
		if (word.equals("CLUSTERED") || word.equals("NONCLUSTERED"))
			word = get_word(fp);
		//  word expected to be "(" at this point
		fields = get_field_list(fp);
		word = get_word(fp);
		if (word.equals("WITH")) {
			word = get_word(fp);
			if (word.equals("("))
				get_field_list(fp);
			else {
				if (!word.equals("FILLFACTOR")) // eat "FILLFACTOR"
				{
					System.err.println("Expected \"FILLFACTOR\" but got " + word);
					System.exit(11);
				}
				get_word(fp);  //  eat "="
				get_word(fp);  //  eat number
			}
			word = get_word(fp);
		}
		if (word.equals("ON")) {
			get_word(fp);  //  eat "PRIMARY"
			word = get_word(fp);
			if (word.equals("TEXTIMAGE_ON")) {
				word = get_word(fp);  //  eat "PRIMARY"
				word = get_word(fp);
			}
		}
		if (word.equals(")"))
			fp.pushc(')');
		schema.addLast(new Index(cname, tname, only, "UNIQUE", false, fields));
		return word;
	}

	private static void parse_alter(FileRead fp, Schema schema) {
		String word = get_word(fp);  // "TABLE"
		boolean only = false;
		String tname;
		String cname;
		LinkedList<String> fields;
		LinkedList<String> fields2;
		String tname2;
		String dflt;
		int n;
		LinkedList<String> lst;

		tname = drop_dbo(get_word(fp)); //  table name
		word = get_word(fp);
		if (word.equals("WITH")) {
			get_word(fp);  //  "NOCHECK"
			word = get_word(fp);
		} else if (word.equals("CHECK")) {
			word = get_word(fp);  //  assume "CONSTRAINT"
			if (!word.equals("CONSTRAINT")) {
				System.err.println("Expected \"CONSTRAINT\" but got " + word + " in ALTER TABLE " + tname);
				System.exit(11);
			}
			word = get_word(fp);  //  assume constraint name
			word = get_word(fp);  //  assume "GO"
			if (!"GO".equals(word) && !"".equals(word)) {
				System.err.println("Expected \"GO\" but got " + word + " in ALTER TABLE " + tname);
				System.exit(11);
			}
			return;
		}

		//  it is assumed that word now has "ADD"
		word = get_word(fp);
		while (true) {
			//  word = "CONSTRAINT", ",", or "GO"
			if (word.equals(",")) {
				word = get_word(fp);
				continue;  //  get next constraint
			}
			if (word.equals("GO") || word.equals(""))
				return;
			//  assume "CONSTRAINT"
			cname = get_word(fp);  //  the constraint name
			word = get_word(fp);   //  "PRIMARY", "DEFAULT", "CHECK", "UNIQUE", or "FOREIGN"
			if (word.equals("PRIMARY"))
				word = parse_primary_key(fp, cname, tname, schema);
			else if (word.equals("DEFAULT")) {
				get_word(fp);  //  eat "("
				dflt = get_word(fp);  //  value or another "(" - used in negative numbers
				if (dflt.equals("("))
					dflt = get_word(fp);
				word = get_word(fp);  //  ")"
				word = get_word(fp);  //  ")" or "FOR"
				if (word.equals(")"))
					word = get_word(fp);
				//  word has "FOR"
				word = get_word(fp);
				Table table = schema.find_table(tname);
				if (table != null) {
					Column col = (Column) table.find(word);
					if ((Object) col != null)
						col.set_default(dflt, cname);
				}
				word = get_word(fp);
			} else if (word.equals("CHECK")) {
				get_word(fp);  //  eat "("
				n = 1;
				lst = new LinkedList<String>();
				while (true) {
					word = get_word(fp);
					if (word.equals("("))
						n++;
					else if (word.equals(")"))
						n--;
					lst.addLast(word);
					if (n > 0)
						continue;
					else
						break;
				}
				Check con = new Check(Schema.Microsoft, cname, tname);
				Table table = schema.find_table(tname);
				con.set_constraint(lst);
				if (table != null)
					table.add_column(con);
				word = get_word(fp);
			} else if (word.equals("UNIQUE"))
				word = parse_unique(fp, cname, tname, schema);
			else if (word.equals("FOREIGN")) {
				get_word(fp);  //  eat "KEY"
				get_word(fp);  //  eat "("
				fields = get_field_list(fp);
				get_word(fp);  //  eat "REFERENCES"
				tname2 = drop_dbo(get_word(fp));
				get_word(fp);  //  eat "("
				fields2 = get_field_list(fp);
				schema.addLast(new ForeignKey(cname, tname, only, fields, tname2, fields2));
				word = get_word(fp);
			} else {
				eat_statement(fp);
				break;
			}
		}
	}

	private static LinkedList<String> get_field_list(FileRead fp) {
		LinkedList<String> lst = new LinkedList<String>();
		String word;

		while (true) {
			word = get_word(fp);
			if (word.equals(")") || word.equals(""))
				return lst;
			if (!word.equals(",") && !word.equals("ASC") && !word.equals("DESC"))
				lst.addLast(word);
			else if (word.equals("DESC")  &&  !lst.isEmpty()) {
				String tmp = lst.removeLast();
				tmp = tmp + " " + word;
				lst.addLast(tmp);
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
		} while (!word.equals("") && !word.equals("GO") && !word.equals(";"));
		return null;
	}

	private static String get_word(FileRead fp) {
		return get_word((char) 0, fp);
	}

	private static String get_word(char c, FileRead fp) {
		if (c == '[')
			c = (char) 0;
		if (is_sep(c))
			return String.valueOf(c);
		char nc;
		if (c == 'N') {			//  change N'text'  into  'text'
			nc = fp.peekc();
			if (nc == '\'')
					c = fp.readc();
		}
		char quotec = c == '\'' ? c : (char) 0;
		StringBuilder word = new StringBuilder();
		if (c != (char) 0)
			word.append(c);
		while (true) {
			c = fp.readc();
			if (c == (char) 0)
				return word.toString();
			if (c == 'N') {			//  change N'text'  into  'text'
				nc = fp.peekc();
				if (nc == '\'')
						c = fp.readc();
			}
			if (quotec != (char) 0) {
				nc = fp.peekc();
				if (c == quotec && nc != quotec) {
					word.append(c);
					return word.toString();
				}
				word.append(c);
				if (c == quotec) {
					word.append(c);
					fp.readc();  //  throw it away
				}
				continue;
			} else if (c == '[' || c == ']')
				continue;
			if (c == '\'')
				if (word.length() == 0) {
					word.append(c);
					quotec = c;
					continue;
				} else {
					fp.pushc(c);
					return word.toString();
				}
			if (is_sep(c))
				if (word.length() == 0)
					return String.valueOf(c);
				else {
					fp.pushc(c);
					return word.toString();
				}
			if (is_space(c))
				if (word.length() == 0)
					continue;
				else
					return word.toString();
			word.append(c);
		}
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

	private static void skip_comment(FileRead fp) {
		char c, pc = ' ';
		while ((char) 0 != (c = fp.readc())) {
			if (c == '/' && pc == '*')
				break;
			pc = c;
		}
	}
}
