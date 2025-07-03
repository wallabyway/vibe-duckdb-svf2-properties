import duckdb
import sys

def main():
    # Check command line arguments
    query_dbid = 1  # Default to dbid = 1
    if len(sys.argv) > 1:
        try:
            query_dbid = int(sys.argv[1])
        except ValueError:
            print("Error: dbid must be an integer")
            sys.exit(1)
    
    print(f"Querying dbid: {query_dbid}")
    
    # Connect to DuckDB and load parquet files
    con = duckdb.connect(':memory:')
    
    # Load parquet files from pq folder
    con.sql("CREATE TABLE ids AS SELECT * FROM read_parquet('pq/ids.parquet')")
    con.sql("CREATE TABLE attrs AS SELECT * FROM read_parquet('pq/attrs.parquet')")
    con.sql("CREATE TABLE vals AS SELECT * FROM read_parquet('pq/vals.parquet')")
    con.sql("CREATE TABLE av_pairs AS SELECT * FROM read_parquet('pq/av_pairs.parquet')")
    con.sql("CREATE TABLE properties AS SELECT * FROM read_parquet('pq/properties.parquet')")
    
    # Query the specific dbid
    print(f"\n=== Properties for dbid={query_dbid} ===")
    result = con.sql("""
    SELECT i.dbid, i.path, i.name, a.internal, a.name as attr_name, v.val
    FROM ids i
    JOIN av_pairs p ON i.dbid = p.dbid
    JOIN attrs a ON p.attr_id = a.attr_id
    JOIN vals v ON p.val_id = v.val_id
    WHERE i.dbid = ?
    ORDER BY a.attr_id
    """, params=[query_dbid]).df()
    
    if result.empty:
        print(f"No properties found for dbid={query_dbid}")
    else:
        print(result)
    
    # Also show a summary of the object
    print(f"\n=== Summary for dbid={query_dbid} ===")
    summary = con.sql("""
    SELECT dbid, path, name
    FROM ids
    WHERE dbid = ?
    """, params=[query_dbid]).df()
    
    if not summary.empty:
        print(f"Object: {summary.iloc[0]['name']} (path: {summary.iloc[0]['path']})")
    else:
        print(f"Object not found for dbid={query_dbid}")

if __name__ == "__main__":
    main() 