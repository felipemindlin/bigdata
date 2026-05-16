# Cassandra CQL Execution Results

Executed commands:

- Started a local Cassandra container `cassandra-local` (image: cassandra:4.1)
- Waited until `cqlsh` responded
- Loaded `cassandra/schema.cql`
- Ran `cassandra/queries_minimas.cql`

Terminal output (captured):

```
docker rm -f cassandra-local 2>/dev/null || true && docker run --name cassandra-local -d -p 9042:9042 cassandra:4.1 && for i in $(seq 1 60); do docker exec cassandra-local cqlsh -e "describe keyspaces" && break || sleep 5; done && docker cp cassandra/schema.cql cassandra-local:/schema.cql && docker exec -i cassandra-local cqlsh -f /schema.cql && docker cp cassandra/queries_minimas.cql cassandra-local:/queries_minimas.cql && docker exec -i cassandra-local cqlsh -f /queries_minimas.cql
cassandra-local
2300daa3fecaafe4f71a80d51684b5695214253343c34f8712c25bae0ec3bfa3
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})
Connection error: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionRefusedError(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})

system       system_distributed  system_traces  system_virtual_schema
system_auth  system_schema       system_views 

Successfully copied 2.56kB to cassandra-local:/schema.cql
Successfully copied 2.56kB to cassandra-local:/queries_minimas.cql

 usage_date | cost_usd | requests | genai_tokens | carbon_kg
------------+----------+----------+--------------+-----------


 (0 rows)

 service | cost_14d_usd
---------+--------------


 (0 rows)

```

Interpretation:

- Cassandra was started in Docker (container `cassandra-local`).
- The container needed a short startup time (several connection attempts observed).
- The schema file was loaded successfully.
- The minimal queries ran but returned empty result sets on the synthetic data.

Next steps you can take (optional):

- If you want real query results, run the pipeline against realistic/full landing data and reload gold into Cassandra using `scripts/run_mvp.py --with-cassandra` or `src/pipeline/cassandra_loader.py`.
- If you prefer a system package install, we can debug the apt repository/GPG errors you observed (network/repo key issues).
