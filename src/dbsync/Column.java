package dbsync;

import java.util.Iterator;
import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class Column implements ISchemaElement {

	private final String name;
	private final String table;
	private LinkedList<String> type;
	private String default_val;
	private String cname;  // constraint name - Microsoft treats default values as constraints
	private boolean not_null;
	private final int dbtype;

	public Column(int dbtyp, String nm, String tab) {
		name = nm;
		table = tab;
		dbtype = dbtyp;
		not_null = false;
	}

	public boolean Equals(Object other) {
		return Equals(this, (Column) other);
	}

	private LinkedList<String> get_fixed_type() {
		if (dbtype == Schema.Microsoft)
			return fix_type_microsoft(type);
		return type;
	}

	private static String list_fix_timestamp(LinkedList<String> type, LinkedList<String> type2) {
		String r = type.getFirst();
		if (r.equals("timestamp") && type.size() != type2.size())
			return r;
		return dbsync.Main.list_to_string(type);
	}

	public static boolean Equals(Column a, Column b) {
		String at = list_fix_timestamp(a.type, b.type);
		String bt = list_fix_timestamp(b.type, a.type);
//            System.out.println(a.table + ", " + a.name + " - " + at + " - " + bt);
		return a.name.equalsIgnoreCase(b.name) &&
				a.table.equalsIgnoreCase(b.table) &&
				//                a.default_val == b.default_val &&
				a.not_null == b.not_null &&
				at.equalsIgnoreCase(bt);
	}

	public int GetHashCode() {
		return name.hashCode();
	}

	@Override
	public String get_name() {
		return name;
	}

	public String get_default() {
		return default_val;
	}

	public void set_type(LinkedList<String> typ) {
		type = typ;
	}

	public LinkedList<String> get_type() {
		return type;
	}

	public void set_default(String dflt, String constraint) {
		default_val = dflt;
		cname = constraint;
	}

	public void set_default(String dflt) {
		default_val = dflt;
	}

	public void set_not_null(boolean flg) {
		not_null = flg;
	}

	public void add_inside_postgres() {
		System.out.print("\"" + name + "\" ");
		System.out.print(dbsync.Main.list_to_string(type));
		if (default_val != null)
			System.out.print(" DEFAULT " + default_val);
		if (not_null)
			System.out.print(" NOT NULL");
	}

	private static LinkedList<String> fix_type_oracle(LinkedList<String> typ) {
		if (typ == null)
			return typ;
		Iterator<String> lnk = typ.iterator();
		String itm = lnk.next();
		if (itm == null)
			return typ;
		if (itm.equals("timestamp") ||
				itm.equals("date")) {
			LinkedList<String> res = new LinkedList<String>();
			res.addLast(itm);
			if (!lnk.hasNext())
				return res;
			itm = lnk.next();
			if (itm.equals("without"))
//                    res.AddLast("with");
//                    res.AddLast("local");
//                    res.AddLast("time");
//                    res.AddLast("zone");
				return res;
		}
		if (itm.equals("character")) {
			if (!lnk.hasNext())
				return typ;
			itm = lnk.next();
			if (itm.equals("varying")) {
				LinkedList<String> res = new LinkedList<String>();
				res.addLast("varchar2");
				while (lnk.hasNext())
					res.addLast(lnk.next());
				return res;
			}
		}
		if (itm.equals("text")) {
			LinkedList<String> res = new LinkedList<String>();
			res.addLast("clob");
			return res;
		}
		if (itm.equals("smallint")) {
			LinkedList<String> res = new LinkedList<String>();
			res.addLast("NUMBER(5)");
			return res;
		}
		if (itm.equals("integer")) {
			LinkedList<String> res = new LinkedList<String>();
			res.addLast("NUMBER(10)");
			return res;
		}
		if (itm.equals("bytea")) {
			LinkedList<String> res = new LinkedList<String>();
			res.addLast("blob");
			return res;
		}
		return typ;
	}

	public void add_inside_oracle() {
		System.out.print("\"" + name.toUpperCase() + "\" ");
		System.out.print(dbsync.Main.list_to_string(fix_type_oracle(type)));
		if (default_val != null)
			System.out.print(" DEFAULT " + default_val);
		if (not_null)
			System.out.print(" NOT NULL");
	}

	private static LinkedList<String> fix_type_microsoft(LinkedList<String> typ) {
		if (typ == null)
			return typ;
		String first = typ.getFirst();
		if (first == null)
			return typ;
		if (first.equals("timestamp") ||
				first.equals("date") ||
				first.equals("time")) {
			typ = new LinkedList<String>();
			typ.addLast("datetime");
		}
		if (first.equals("bytea")) {
			typ = new LinkedList<String>();
			typ.addLast("image");
		}
		return typ;
	}

	public void add_inside_microsoft() {
		System.out.print("[" + name + "] ");
		System.out.print(dbsync.Main.list_to_string(fix_type_microsoft(type)));
		if (default_val != null)
			System.out.print(" DEFAULT " + default_val);
		if (not_null)
			System.out.print(" NOT NULL");
	}

	@Override
	public void add_postgres() {
		System.out.print("ALTER TABLE \"" + table + "\" ADD COLUMN \"" + name + "\" " + dbsync.Main.list_to_string(type));
		if (default_val != null)
			System.out.print(" DEFAULT " + default_val);
		if (not_null)
			System.out.print(" NOT NULL");
		System.out.println(";\n");
	}

	@Override
	public void add_microsoft() {
		System.out.print("ALTER TABLE [dbo].[" + table + "] ADD [" + name + "] " + dbsync.Main.list_to_string(fix_type_microsoft(type)));
		if (default_val != null)
			System.out.print(" DEFAULT " + default_val);
		else if (not_null) {
			String typ = type.getFirst();
			if (typ.endsWith("character"))
				System.out.print(" DEFAULT ''");
			else
				System.out.print(" DEFAULT 0");
		}
		if (not_null)
			System.out.print(" NOT NULL");
		System.out.println("\nGO\n");
	}

	@Override
	public void add_oracle() {
		System.out.print("ALTER TABLE \"" + table.toUpperCase() + "\" ADD COLUMN \"" + name.toUpperCase() + "\" " + dbsync.Main.list_to_string(type));
		if (default_val != null)
			System.out.print(" DEFAULT " + default_val);
		if (not_null)
			System.out.print(" NOT NULL");
		System.out.println(";\n");
	}

	@Override
	public void drop_postgres() {
		System.out.println("ALTER TABLE \"" + table + "\" DROP COLUMN \"" + name + "\";\n");
	}

	@Override
	public void drop_microsoft() {
		if (default_val != null && cname != null)
			System.out.println("ALTER TABLE [dbo].[" + table + "] DROP [" + cname + "]\nGO\n");
		System.out.println("ALTER TABLE [dbo].[" + table + "] DROP COLUMN [" + name + "]\nGO\n");
	}

	public static void alter_column(Column oldcol, Column newcol, int dbtype) {
		if (dbtype == Schema.Postgres) {
			if (oldcol.not_null == true && newcol.not_null == false)
				System.out.print("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" DROP NOT NULL;\n");
			else if (oldcol.not_null == false && newcol.not_null == true)
				System.out.print("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" SET NOT NULL;\n");
			String at = list_fix_timestamp(oldcol.type, newcol.type);
			String bt = list_fix_timestamp(newcol.type, oldcol.type);
			if (!at.equalsIgnoreCase(bt))
				System.out.print("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" TYPE " +
						dbsync.Main.list_to_string(newcol.type) + ";\n");

		} else if (dbtype == Schema.Microsoft) {
			System.out.print("ALTER TABLE [dbo].[" + newcol.table + "] ALTER COLUMN ");
			System.out.print("[" + newcol.name + "] ");
			System.out.print(dbsync.Main.list_to_string(fix_type_microsoft(newcol.type)));
			if (newcol.not_null)
				System.out.print(" NOT NULL");
			System.out.println("\nGO\n");
		}
	}

	public static void alter_default(Column oldcol, Column newcol, int dbtype) {
		if (dbtype == Schema.Postgres) {
			System.out.print("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" ");
			if (newcol.default_val == null)
				System.out.print("DROP DEFAULT");
			else
				System.out.print("SET DEFAULT " + newcol.default_val);
			System.out.println(";\n");
		} else if (dbtype == Schema.Microsoft) {
			if (oldcol.default_val != null && oldcol.cname != null)
				System.out.println("ALTER TABLE [dbo].[" + oldcol.table + "] DROP [" + oldcol.cname + "]\nGO\n");
			if (newcol.default_val != null) {
				System.out.print("ALTER TABLE [dbo].[" + newcol.table + "] ADD CONSTRAINT [");
				if (newcol.cname == null)
					newcol.cname = newcol.table + "_" + newcol.name + "_" + "dflt";
				System.out.print(newcol.cname);
				System.out.println("] DEFAULT " + newcol.default_val + " FOR [" + newcol.name + "]\nGO\n");
			}
		}
	}
}
