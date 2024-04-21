package dbsync;

/**
 *
 * @author Blake McBride
 */
public interface ISchemaElement {

	void add_postgres();

	void add_microsoft();

	void add_oracle();

	String get_name();

	void drop_postgres();

	void drop_microsoft();
}
