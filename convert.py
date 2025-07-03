import duckdb, struct, json, os, time

def read_varint(data, offset):
    value = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return value, offset

def process_avs_data():
    with open("otg/avs.idx", "rb") as f:
        data = f.read()
        idx = struct.unpack(f"<{len(data)//4}I", data)
    with open("otg/avs.pack", "rb") as f:
        pack = f.read()
    
    all_pairs = []
    
    for dbid in range(len(idx) - 1):
        start = idx[dbid]
        end = idx[dbid + 1] if dbid + 1 < len(idx) else len(pack)
        segment = pack[start:end]
        
        offset = 0
        attr_id = -1
        
        while offset < len(segment):
            attr_delta, offset = read_varint(segment, offset)
            attr_id += attr_delta
            
            val_id, offset = read_varint(segment, offset)
            
            all_pairs.append((dbid, attr_id, val_id))
    
    return all_pairs

def main():
    t0 = time.time()
    os.makedirs('pq', exist_ok=True)
    
    all_pairs = process_avs_data()
    
    con = duckdb.connect('pq/ice.duckdb')
    con.sql("LOAD json")
    
    con.sql("DROP TABLE IF EXISTS ids")
    con.sql("CREATE TABLE ids AS SELECT row_number() OVER () - 1 dbid, json::VARCHAR path FROM read_json_auto('otg/ids.json', format='array')")
    
    with open('otg/attrs.json') as f:
        attrs = [[i]+a for i,a in enumerate(json.load(f)[1:])]
    con.sql("DROP TABLE IF EXISTS attrs")
    con.sql("CREATE TABLE attrs (attr_id INTEGER, internal VARCHAR, name VARCHAR, data_type INTEGER, param4 VARCHAR, param5 VARCHAR, param6 VARCHAR, flags INTEGER, param8 INTEGER)")
    con.executemany("INSERT INTO attrs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", attrs)
    
    con.sql("DROP TABLE IF EXISTS vals")
    con.sql("CREATE TABLE vals AS SELECT row_number() OVER () - 1 val_id, json val FROM read_json_auto('otg/vals.json', format='array')")
    
    con.sql("DROP TABLE IF EXISTS av_pairs")
    con.sql("CREATE TABLE av_pairs (dbid INTEGER, attr_id INTEGER, val_id INTEGER)")
    
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        for dbid, attr_id, val_id in all_pairs:
            f.write(f"{dbid},{attr_id},{val_id}\n")
        temp_csv = f.name
    
    con.sql(f"COPY av_pairs FROM '{temp_csv}' (FORMAT CSV)")
    os.unlink(temp_csv)
    
    con.sql("COPY (SELECT i.dbid, i.path, COALESCE(CAST(name_val.val AS VARCHAR), '') as name FROM ids i LEFT JOIN (SELECT p.dbid, v.val FROM av_pairs p JOIN vals v ON p.val_id = v.val_id WHERE p.attr_id = 0) name_val ON i.dbid = name_val.dbid ORDER BY i.dbid) TO 'pq/ids.parquet' (FORMAT PARQUET)")
    con.sql("COPY (SELECT i.dbid, i.path, COALESCE(CAST(name_val.val AS VARCHAR), '') as name FROM ids i LEFT JOIN (SELECT p.dbid, v.val FROM av_pairs p JOIN vals v ON p.val_id = v.val_id WHERE p.attr_id = 0) name_val ON i.dbid = name_val.dbid WHERE (LENGTH(i.path) - LENGTH(REPLACE(i.path, '/', '')) <= 1) ORDER BY i.dbid) TO 'pq/toplevel.parquet' (FORMAT PARQUET)")
    con.sql("COPY attrs TO 'pq/attrs.parquet' (FORMAT PARQUET)")
    con.sql("COPY vals TO 'pq/vals.parquet' (FORMAT PARQUET)")
    con.sql("COPY av_pairs TO 'pq/av_pairs.parquet' (FORMAT PARQUET)")
    con.sql("COPY (SELECT i.dbid, i.path, a.internal, a.name, v.val FROM ids i JOIN av_pairs p ON i.dbid = p.dbid JOIN attrs a ON p.attr_id = a.attr_id JOIN vals v ON p.val_id = v.val_id ORDER BY i.dbid, a.attr_id) TO 'pq/properties.parquet' (FORMAT PARQUET)")
    
    print(f"Total time: {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main() 