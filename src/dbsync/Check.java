package dbsync;

import java.util.Iterator;
import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class Check implements ISchemaElement {

	private final String table;
	private final String name;
	private LinkedList<String> constraint;
	private final int dbtype;
	private boolean isDeleted;

	public Check(int dbtyp, String nam, String tname) {
		dbtype = dbtyp;
		name = nam;
		table = Main.remove_public(tname);
		isDeleted = false;
	}

	public boolean Equals(Object other) {
		return Equals(this, (Check) other);
	}

	public static boolean Equals(Check a, Check b) {
		if (!a.table.equalsIgnoreCase(b.table) ||
				!a.name.equalsIgnoreCase(b.name) ||
				a.isDeleted != b.isDeleted)
			return false;

		Iterator<String> an = a.constraint.iterator();
		Iterator<String> bn = b.constraint.iterator();
		while (true) {
			String sa, sb;

			sa = null;
			sb = null;
			if (an.hasNext())
				for (sa = an.next(); sa.equals("(") || sa.equals(")");)
					if (!an.hasNext()) {
						sa = null;
						break;
					} else
						sa = an.next();
			if (bn.hasNext())
				for (sb = bn.next(); sb.equals("(") || sb.equals(")");)
					if (!bn.hasNext()) {
						sb = null;
						break;
					} else
						sb = bn.next();

			if (sa == null && sb != null || sa != null && sb == null)
				return false;
			if (sa == null && sb == null)
				return true;

			if (sa.charAt(0) == '\'') {
				if (!sa.equals(sb))
					return false;
			} else if (!sa.equalsIgnoreCase(sb))
				return false;
		}
	}

	public int GetHashCode() {
		return name.hashCode();
	}

	@Override
	public String get_name() {
		return name;
	}

	public void set_constraint(LinkedList<String> typ) {
		constraint = typ;
	}

	public LinkedList<String> get_constraint() {
		return constraint;
	}

	public boolean isElement(String field) {
		for (String fld : constraint)
			if (field.equals(fld))
				return true;
		return false;
	}

	public void add_inside_postgres() {
		System.out.print("CONSTRAINT \"" + name + "\" CHECK " + dbsync.Main.list_to_string(constraint));
	}

	public void add_inside_microsoft() {
		System.out.print("CONSTRAINT [" + name + "] CHECK " + dbsync.Main.list_to_string(constraint));
	}

	private LinkedList<String> fix_oracle_check(LinkedList<String> con) {
		LinkedList<String> res = new LinkedList<String>();
		for (String str : con)
			if (Character.isLetter(str.charAt(0)) && !str.equals("OR") && !str.equals("AND"))
				res.addLast("\"" + str.toUpperCase() + "\"");
			else
				res.addLast(str);
		return res;
	}

	public void add_inside_oracle() {
		System.out.print("CONSTRAINT \"" + name.toUpperCase() + "\" CHECK " + dbsync.Main.list_to_string(fix_oracle_check(constraint)));
	}

	@Override
	public void add_postgres() {
		System.out.println("ALTER TABLE \"" + table + "\" ADD CONSTRAINT \"" + name + "\" CHECK " + dbsync.Main.list_to_string(constraint) + ";\n");
	}

	@Override
	public void add_microsoft() {
		System.out.println("ALTER TABLE [dbo].[" + table + "] ADD CONSTRAINT [" + name + "] CHECK " + dbsync.Main.list_to_string(constraint) + "\nGO\n");
	}

	@Override
	public void add_oracle() {
		System.out.println("ALTER TABLE \"" + table.toUpperCase() + "\" ADD CONSTRAINT \"" + dbsync.Main.limit_name(30, name.toUpperCase()) + "\" CHECK " + dbsync.Main.list_to_string(constraint) + ";\n");
	}

	@Override
	public void drop_postgres() {
		if (!isDeleted) {
			System.out.println("ALTER TABLE \"" + table + "\" DROP CONSTRAINT \"" + name + "\";\n");
			isDeleted = true;
		}
	}

	@Override
	public void drop_microsoft() {
		if (!isDeleted) {
			System.out.println("ALTER TABLE [dbo].[" + table + "] DROP CONSTRAINT [" + name + "]\nGO\n");
			isDeleted = true;
		}
	}

	public static void alter_check(Check oldcol, Check newcol) {
		if (oldcol.dbtype == Schema.Postgres) {
			oldcol.drop_postgres();
			newcol.add_postgres();
		} else if (oldcol.dbtype == Schema.Microsoft) {
			oldcol.drop_microsoft();
			newcol.add_microsoft();
		}
	}
}
