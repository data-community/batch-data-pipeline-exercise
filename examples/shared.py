from airflow.providers.postgres.hooks.postgres import PostgresHook

def normalize_csv(ts, **kwargs):
    import csv
    source_filename = kwargs['source']
    target_filename = kwargs['target']
    header_skipped = False
    with open(source_filename, newline='') as source_file:
        with open(target_filename, "w", newline='') as target_file:
            reader = csv.reader(source_file, delimiter=',')
            writer = csv.writer(target_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if not header_skipped:
                    header_skipped = True
                    continue
                row.append(ts)
                writer.writerow(row)
    return target_filename

def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connection_id = kwargs['connection_id']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.bulk_load(table_name, csv_filepath)
    return table_name