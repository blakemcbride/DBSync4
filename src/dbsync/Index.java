package dbsync;

import java.util.Iterator;
import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class Index implements ISchemaElement {

	private final String table;
	private final String iname;
	private final boolean only;
	private String type;
	private final LinkedList<String> fields;
	private boolean isDeleted;
	private final boolean isIndex;

	public Index(String nam, String tab, boolean on, String typ, boolean isIdx, LinkedList<String> flds) {
		iname = nam;
		table = Main.remove_public(tab);
		only = on;
		type = typ;
		fields = flds;
		isIndex = isIdx;
		isDeleted = false;
	}

	public boolean Equals(Object other) {
		return Equals(this, (Index) other);
	}

	public static boolean Equals(Index a, Index b) {
		if (!a.table.equalsIgnoreCase(b.table) ||
				!a.iname.equalsIgnoreCase(b.iname) ||
				!a.type.equalsIgnoreCase(b.type) ||
				a.fields.size() != b.fields.size() ||
				a.isIndex != b.isIndex ||
				a.isDeleted != b.isDeleted)
			return false;

		Iterator<String> na = a.fields.iterator();
		Iterator<String> nb = b.fields.iterator();
		for (String field : a.fields)
			if (!na.next().equalsIgnoreCase(nb.next()))
				return false;
		return true;
	}

	public int GetHashCode() {
		return iname.hashCode();
	}

	public void set_type(String typ) {
		type = typ;
	}

	public String get_type() {
		return type;
	}

	@Override
	public String get_name() {
		return iname;
	}

	public boolean isElement(String field) {
		for (String fld : fields)
			if (field.equals(fld))
				return true;
		return false;
	}

	@Override
	public void add_postgres() {
		boolean comma = false;
		if (isIndex) {
			if (type.equals("UNIQUE"))
				System.out.print("CREATE UNIQUE INDEX \"" + iname + "\" ON \"" + table + "\" USING btree (");
			else
				System.out.print("CREATE INDEX \"" + iname + "\" ON \"" + table + "\" USING btree (");

			for (String fld : fields) {
				if (comma)
					System.out.print(", ");
				else
					comma = true;

				if (fld.startsWith("upper(") || fld.startsWith("lower("))
					System.out.print(fld.substring(0, 6) + "\"" + fld.substring(6, fld.length() - 1) + "\")");
				else {
					String [] words = fld.split(" ");
					if (words.length == 2)
						System.out.print("\"" + words[0] + "\" " + words[1]);
					else
						System.out.print("\"" + fld + "\"");
				}
			}
			System.out.println(");\n");
		} else {
			System.out.print("ALTER TABLE ONLY \"" + table + "\" ADD CONSTRAINT \"" + iname + "\" " + type + " (");
			for (String fld : fields) {
				if (comma)
					System.out.print(", ");
				else
					comma = true;

				System.out.print("\"" + fld + "\"");
			}

			System.out.println(");\n");
		}

	}

	@Override
	public void add_microsoft() {
		boolean comma = false;
		if (isIndex) {
			if (type.equals("UNIQUE"))
				System.out.print("CREATE UNIQUE INDEX [" + iname + "] ON [dbo].[" + table + "] (");
			else
				System.out.print("CREATE INDEX [" + iname + "] ON [dbo].[" + table + "] (");

			for (String fld : fields) {
				if (comma)
					System.out.print(", ");
				else
					comma = true;

				if (fld.startsWith("upper(") || fld.startsWith("lower("))
					System.out.print("[" + fld.substring(6, fld.length() - 1) + "]");
				else {
					String [] words = fld.split(" ");
					if (words.length == 2)
						System.out.print("[" + words[0] + "] " + words[1]);
					else
						System.out.print("[" + fld + "]");
				}
			}
			System.out.println(")\nGO\n");
		} else {
			System.out.print("ALTER TABLE [dbo].[" + table + "] ADD CONSTRAINT [" + iname + "] " + type + " (");
			for (String fld : fields) {
				if (comma)
					System.out.print(", ");
				else
					comma = true;

				System.out.print("[" + fld + "]");
			}

			System.out.println(")\nGO\n");
		}

	}

	@Override
	public void add_oracle() {
		boolean comma = false;
		if (isIndex) {
			if (type.equals("UNIQUE"))
				System.out.print("CREATE UNIQUE INDEX \"" + dbsync.Main.limit_name(30, iname.toUpperCase()) + "\" ON \"" + table.toUpperCase() + "\" (");
			else
				System.out.print("CREATE INDEX \"" + dbsync.Main.limit_name(30, iname.toUpperCase()) + "\" ON \"" + table.toUpperCase() + "\" (");

			for (String fld : fields) {
				if (comma)
					System.out.print(", ");
				else
					comma = true;

				if (fld.startsWith("upper(") || fld.startsWith("lower("))
					System.out.print(fld.substring(0, 6).toUpperCase() + "\"" + fld.substring(6, fld.length() - 1).toUpperCase() + "\")");
				else
					System.out.print("\"" + fld.toUpperCase() + "\"");

			}
			System.out.println(");\n");
		} else {
			System.out.print("ALTER TABLE \"" + table.toUpperCase() + "\" ADD CONSTRAINT \"" + dbsync.Main.limit_name(30, iname.toUpperCase()) + "\" " + type + " (");
			for (String fld : fields) {
				if (comma)
					System.out.print(", ");
				else
					comma = true;

				System.out.print("\"" + fld.toUpperCase() + "\"");
			}

			System.out.println(");\n");
		}

	}

	@Override
	public void drop_postgres() {
		if (isDeleted)
			return;
		else
			isDeleted = true;

		if (isIndex)
			System.out.println("DROP INDEX \"" + iname + "\";\n");
		else
			System.out.println("ALTER TABLE ONLY \"" + table + "\" DROP CONSTRAINT \"" + iname + "\";\n");

	}

	@Override
	public void drop_microsoft() {
		if (isDeleted)
			return;
		else
			isDeleted = true;

		if (isIndex)
			System.out.println("DROP INDEX [dbo].[" + table + "].[" + iname + "]\nGO\n");
		else
			System.out.println("ALTER TABLE [dbo].[" + table + "] DROP CONSTRAINT [" + iname + "]\nGO\n");
	}
}
