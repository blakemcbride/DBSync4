package dbsync;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Blake McBride
 */
public class Main {

	public static String dashes = "\n\n-- ----------------------------------------------------------\n";
	private static boolean fk_reset = true;

	public static void main(String[] args) {
		if (args.length < 2)
			usage();

		Schema schema1 = null;
		Schema schema2 = null;
		boolean compare = false;

		for (int arg = 0; arg < args.length; arg++)
			if (args[arg].equals("-nfk"))
				fk_reset = false;
			else if (args[arg].equals("-ip")) {
				if (++arg == args.length)
					usage();
				checkFile(args[arg]);
				if (schema1 == null) {
					schema1 = ParsePostgres.parse_postgres_schema(args[arg]);
					schema1 = sort_schema(schema1);
				} else {
					compare = true;
					schema2 = ParsePostgres.parse_postgres_schema(args[arg]);
					schema2 = sort_schema(schema2);
				}
			} else if (args[arg].equals("-im")) {
				if (++arg == args.length)
					usage();
				checkFile(args[arg]);
				if (schema1 == null) {
					schema1 = ParseMicrosoft.parse_microsoft_schema(args[arg]);
					schema1 = sort_schema(schema1);
				} else {
					compare = true;
					schema2 = ParseMicrosoft.parse_microsoft_schema(args[arg]);
					schema2 = sort_schema(schema2);
				}
			} else if (args[arg].equals("-tp")) {  // convert microsoft insert statements to postgres format
				ParseMicrosoft.microsoft_inserts_to_postgres(args[arg+1]);
			} else if (args[arg].equals("-om"))
				if (compare) {
					if (fk_reset)
						generate_drop_foreign_keys_microsoft(schema1);
					table_diff_microsoft(schema1, schema2);
					if (fk_reset)
						generate_foreign_keys_microsoft(schema2);
				} else
					display_microsoft_schema(schema1);
			else if (args[arg].equals("-op"))
				if (compare) {
					if (fk_reset)
						generate_drop_foreign_keys_postgres(schema1);
					table_diff_postgres(schema1, schema2);
					if (fk_reset)
						generate_foreign_keys_postgres(schema2);
				} else
					display_postgres_schema(schema1);
			else if (args[arg].equals("-oo"))
				display_oracle_schema(schema1);
			else
				usage();
		System.exit(0);
	}

	private static void usage() {
		System.err.println("Usage:  DBSync4  -im|-ip  input-schema-file  -om|-op|-oo");
		System.err.println("          or");
		System.err.println("        DBSync4  [-nfk]  -im|-ip  old-schema-file  -im|-ip  new-schema-file  -om|-op");
		System.err.println("          or");
		System.err.println("        DBSync4  -tp  microsoft-insert-statement-file");
		System.exit(10);
	}

	private static void table_diff_postgres(Schema oldschema, Schema newschema) {
		System.out.println("\n--  Remove indexes and checks\n");
		for (ISchemaElement olde : oldschema) {
			ISchemaElement newe = newschema.find(olde.get_name());
			if (newe == null) {
				if (olde instanceof Index || olde instanceof Check)
					olde.drop_postgres();
			} else if (newe instanceof Index)
				if (!((Index) olde).Equals((Index) newe))
					olde.drop_postgres();
		}

		System.out.println("\n--  Add new tables\n");
		for (ISchemaElement newe : newschema) {
			ISchemaElement olde = oldschema.find(newe.get_name());
			if (olde == null && newe instanceof Table)
				newe.add_postgres();
		}

		System.out.println("\n--  Add new columns\n");
		for (ISchemaElement newe : newschema) {
			ISchemaElement olde = oldschema.find(newe.get_name());
			if (olde == null)
				continue;
			else if (newe.getClass() != olde.getClass()) {
				System.err.println(newe.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe instanceof Table)
				for (ISchemaElement newcol : ((Table) newe).get_columns()) {
					ISchemaElement oldcol = ((Table) olde).find(newcol.get_name());
					if (oldcol == null) {
						if (!(newcol instanceof Check))
							newcol.add_postgres();
					} else if (newcol.getClass() != oldcol.getClass()) {
						System.err.println(newcol.get_name() + " names two different types of objects.");
						System.exit(10);
					}
				}
		}

		System.out.println("\n--  Change existing columns\n");
		for (ISchemaElement newe : newschema) {
			ISchemaElement olde = oldschema.find(newe.get_name());
			if (olde == null)
				continue;
			else if (newe.getClass() != olde.getClass()) {
				System.err.println(newe.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe instanceof Table)
				for (ISchemaElement newcol : ((Table) newe).get_columns()) {
					ISchemaElement oldcol = ((Table) olde).find(newcol.get_name());
					if (oldcol == null)
						continue;
					else if (newcol.getClass() != oldcol.getClass()) {
						System.err.println(newcol.get_name() + " names two different types of objects.");
						System.exit(10);
					} else if (newcol instanceof Column) {
						String d1 = ((Column) newcol).get_default();
						String d2 = ((Column) oldcol).get_default();
						if (d1 != null || d2 != null)
							if (d1 == null && d2 != null || d1 != null && d2 == null || !d1.equals(d2))
								Column.alter_default((Column) oldcol, (Column) newcol, Schema.Postgres);
						if (!((Column) newcol).Equals((Column) oldcol)) {
							for (ISchemaElement index : oldschema)
								if (index instanceof Index && ((Index) index).isElement(((Column) oldcol).get_name()))
									((Index) index).drop_postgres();
							Column.alter_column((Column) oldcol, (Column) newcol, Schema.Postgres);
						}

					}
				}
		}

		System.out.println("\n--  Remove tables\n");

		for (ISchemaElement olde2 : oldschema) {
			ISchemaElement newe2 = newschema.find(olde2.get_name());
			if (newe2 == null && olde2 instanceof Table)
				olde2.drop_postgres();
		}

		System.out.println("\n--  Drop columns\n");
		for (ISchemaElement olde3 : oldschema) {
			ISchemaElement newe3 = newschema.find(olde3.get_name());
			if (newe3 == null)
				continue;
			else if (newe3.getClass() != olde3.getClass()) {
				System.err.println(newe3.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe3 instanceof Table)
				for (ISchemaElement oldcol : ((Table) olde3).get_columns()) {
					ISchemaElement newcol = ((Table) newe3).find(oldcol.get_name());
					if (newcol == null) {
						for (ISchemaElement chkcol : ((Table) olde3).get_columns())
							if (chkcol instanceof Check && ((Check) chkcol).isElement(oldcol.get_name()))
								chkcol.drop_postgres();
						oldcol.drop_postgres();
					}
				}
		}

		System.out.println("\n--  Add new indexes and checks\n");

		for (ISchemaElement newe4 : newschema) {
			ISchemaElement olde4 = oldschema.find(newe4.get_name());
			if (olde4 == null) {
				if (!(newe4 instanceof ForeignKey) && !(newe4 instanceof Table))
					newe4.add_postgres();
			} else if (newe4.getClass() != olde4.getClass()) {
				System.err.println(newe4.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe4 instanceof Table)
				for (ISchemaElement newcol : ((Table) newe4).get_columns()) {
					ISchemaElement oldcol = ((Table) olde4).find(newcol.get_name());
					if (oldcol == null) {
						if (newcol instanceof Check)
							newcol.add_postgres();
					} else if (newcol.getClass() != oldcol.getClass()) {
						System.err.println(newcol.get_name() + " names two different types of objects.");
						System.exit(10);
					} else if (newcol instanceof Check)
						if (!((Check) newcol).Equals((Check) oldcol))
							Check.alter_check((Check) oldcol, (Check) newcol);
				}
			else if (newe4 instanceof Index)
				if (!((Index) olde4).Equals((Index) newe4))
					newe4.add_postgres();
		}

		System.out.println(dashes);
	}

	private static void table_diff_microsoft(Schema oldschema, Schema newschema) {
		System.out.println("\n--  Remove indexes and checks\n");
		for (ISchemaElement olde : oldschema) {
			ISchemaElement newe = newschema.find(olde.get_name());
			if (newe == null) {
				if (olde instanceof Index || olde instanceof Check)
					olde.drop_microsoft();
			} else if (newe instanceof Index)
				if (!((Index) olde).Equals((Index) newe))
					olde.drop_microsoft();
		}

		System.out.println("\n--  Add new tables\n");

		for (ISchemaElement newe : newschema) {
			ISchemaElement olde = oldschema.find(newe.get_name());
			if (olde == null && newe instanceof Table)
				newe.add_microsoft();
		}

		System.out.println("\n--  Add new columns\n");
		for (ISchemaElement newe : newschema) {
			ISchemaElement olde = oldschema.find(newe.get_name());
			if (olde == null)
				continue;
			else if (newe.getClass() != olde.getClass()) {
				System.err.println(newe.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe instanceof Table)
				for (ISchemaElement newcol : ((Table) newe).get_columns()) {
					ISchemaElement oldcol = ((Table) olde).find(newcol.get_name());
					if (oldcol == null) {
						if (!(newcol instanceof Check))
							newcol.add_microsoft();
					} else if (newcol.getClass() != oldcol.getClass()) {
						System.err.println(newcol.get_name() + " names two different types of objects.");
						System.exit(10);
					}

				}
		}

		System.out.println("\n--  Change existing columns\n");
		for (ISchemaElement newe : newschema) {
			ISchemaElement olde = oldschema.find(newe.get_name());
			if (olde == null)
				continue;
			else if (newe.getClass() != olde.getClass()) {
				System.err.println(newe.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe instanceof Table)
				for (ISchemaElement newcol : ((Table) newe).get_columns()) {
					ISchemaElement oldcol = ((Table) olde).find(newcol.get_name());
					if (oldcol == null)
						continue;
					else if (newcol.getClass() != oldcol.getClass()) {
						System.err.println(newcol.get_name() + " names two different types of objects.");
						System.exit(10);
					} else if (newcol instanceof Column) {
						String d1 = ((Column) newcol).get_default();
						String d2 = ((Column) oldcol).get_default();
						if (d1 != null || d2 != null)
							if (d1 == null && d2 != null || d1 != null && d2 == null || !d1.equals(d2))
								Column.alter_default((Column) oldcol, (Column) newcol, Schema.Microsoft);

						if (!((Column) newcol).Equals((Column) oldcol)) {
							for (ISchemaElement index : oldschema)
								if (index instanceof Index && ((Index) index).isElement(((Column) oldcol).get_name()))
									((Index) index).drop_microsoft();
							Column.alter_column((Column) oldcol, (Column) newcol, Schema.Microsoft);
						}

					}
				}
		}

		System.out.println("\n--  Remove tables\n");
		for (ISchemaElement olde : oldschema) {
			ISchemaElement newe = newschema.find(olde.get_name());
			if (newe == null && olde instanceof Table)
				olde.drop_microsoft();
		}

		System.out.println("\n--  Drop columns\n");
		for (ISchemaElement olde : oldschema) {
			ISchemaElement newe = newschema.find(olde.get_name());
			if (newe == null)
				continue;
			else if (newe.getClass() != olde.getClass()) {
				System.err.println(newe.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe instanceof Table)
				for (ISchemaElement oldcol : ((Table) olde).get_columns()) {
					ISchemaElement newcol = ((Table) newe).find(oldcol.get_name());
					if (newcol == null) {
						for (ISchemaElement chkcol : ((Table) olde).get_columns())
							if (chkcol instanceof Check && ((Check) chkcol).isElement(oldcol.get_name()))
								chkcol.drop_microsoft();
						oldcol.drop_microsoft();
					}
				}
		}
		System.out.println("\n--  Add new indexes and checks\n");

		for (ISchemaElement newe2 : newschema) {
			ISchemaElement olde2 = oldschema.find(newe2.get_name());
			if (olde2 == null) {
				if (!(newe2 instanceof ForeignKey) && !(newe2 instanceof Table))
					newe2.add_microsoft();
			} else if (newe2.getClass() != olde2.getClass()) {
				System.err.println(newe2.get_name() + " names two different types of objects.");
				System.exit(10);
			} else if (newe2 instanceof Table)
				for (ISchemaElement newcol : ((Table) newe2).get_columns()) {
					ISchemaElement oldcol = ((Table) olde2).find(newcol.get_name());
					if (oldcol == null) {
						if (newcol instanceof Check)
							newcol.add_microsoft();
					} else if (newcol.getClass() != oldcol.getClass()) {
						System.err.println(newcol.get_name() + " names two different types of objects.");
						System.exit(10);
					} else if (newcol instanceof Check)
						if (!((Check) newcol).Equals((Check) oldcol))
							Check.alter_check((Check) oldcol, (Check) newcol);
				}
			else if (newe2 instanceof Index)
				if (!((Index) olde2).Equals((Index) newe2))
					newe2.add_microsoft();
		}

		System.out.println(dashes);
	}

	private static void generate_drop_foreign_keys_postgres(Schema schema) {
		for (ISchemaElement elm : schema)
			if (elm instanceof ForeignKey)
				elm.drop_postgres();
		System.out.println("\n\n");
	}

	private static void generate_drop_foreign_keys_microsoft(Schema schema) {
		for (ISchemaElement elm : schema)
			if (elm instanceof ForeignKey)
				elm.drop_microsoft();
		System.out.println("\n\n");
	}

	private static void generate_foreign_keys_postgres(Schema schema) {
		for (ISchemaElement elm : schema)
			if (elm instanceof ForeignKey)
				((ForeignKey) elm).add_postgres();
	}

	private static void generate_foreign_keys_microsoft(Schema schema) {
		for (ISchemaElement elm : schema)
			if (elm instanceof ForeignKey)
				((ForeignKey) elm).add_microsoft();
	}

	private static void checkFile(String file) {
		if (!(new File(file)).exists()) {
			System.err.println("File " + file + " does not exist.");
			System.exit(-1);
		}
	}

	/* The following method was needed to make Java sort like C#.
	 * This is so we can do a diff to verify the conversion from C# to Java.
	 * Once we are sure this Java version works, we can get rid of the special sort.
	 * */
	private static String fix_string(String s) {
	//	s = s.replace('_', ' ');
		return s;
	}
	
	private static final Comparator<ISchemaElement> compareSchema = new Comparator<ISchemaElement>() {

		@Override
		public int compare(ISchemaElement e1, ISchemaElement e2) {
			if (e1.getClass() == e2.getClass()) {
				String n1 = e1.get_name();
				String n2 = e2.get_name();
				if (e1 instanceof Table || e1 instanceof Index) {
					n1 = fix_string(n1);
					n2 = fix_string(n2);
				}
				return n1.compareTo(n2);
			}

			if (e1 instanceof Table)
				return -1;
			if (e2 instanceof Table)
				return 1;
			if (e1 instanceof Index)
				return -1;
			if (e2 instanceof Index)
				return 1;
			if (e1 instanceof ForeignKey)
				return -1;
			if (e2 instanceof ForeignKey)
				return 1;
			return -1;
		}
	};

	private static Schema sort_schema(Schema schema) {
		List<ISchemaElement> lst = new LinkedList<ISchemaElement>();
		for (ISchemaElement elm : schema)
			lst.add(elm);

		Collections.sort(lst, compareSchema);

		Schema schema2 = new Schema(schema.get_dbtype());
		for (ISchemaElement elm : lst)
			schema2.addLast(elm);
		return schema2;
	}

	private static void display_postgres_schema(Schema schema) {
		for (ISchemaElement elm : schema)
			elm.add_postgres();
	}

	private static void display_microsoft_schema(Schema schema) {
		for (ISchemaElement elm : schema)
			elm.add_microsoft();
	}

	private static void display_oracle_schema(Schema schema) {
		for (ISchemaElement elm : schema)
			elm.add_oracle();
	}

	public static String strip_quotes(String name) {
		if (name.charAt(0) == '"') {
			name = name.substring(1);
			return name.substring(0, name.length() - 1);
		}
		return name;
	}

	public static String quote_two_names(String name) {
		int i = name.indexOf('.');
		if (i >= 0)
			return "\"" + name.substring(0, i) + "\".\"" + name.substring(i + 1) + "\"";
		else
			return "\"" + name + "\"";
	}

	public static String list_to_string(LinkedList<String> lst) {
		if (lst == null)
			return "";
		StringBuilder res = new StringBuilder();

		for (String str : lst) {
			char lchar, fchar;
			if (res.length() == 0)
				lchar = ' ';
			else
				lchar = res.charAt(res.length() - 1);

			fchar = str.charAt(0);
			if (Character.isLetterOrDigit(lchar) && Character.isLetterOrDigit(fchar))
				res.append(' ');
			res.append(str);
		}
		return res.toString();
	}

	public static String limit_name(int len, String str) {
		if (str.length() <= len)
			return str;
		return str.substring(0, len);
	}
	
	public static String remove_public(final String nam) {
		if (nam != null  &&  nam.startsWith("public."))
			return nam.substring(7);
		else
			return nam;
	}
}


